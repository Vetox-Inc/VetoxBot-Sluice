package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	stateVersion      = 1
	stateSaveInterval = 5 * time.Second
	// maxStateFileBytes is far above a full state, about 1 MiB with every webhook slot in use.
	maxStateFileBytes = 16 << 20
)

// savedState is what STATE_FILE keeps across restarts: the memory that protects this IP from
// Discord's restrictions, which a restart would otherwise forget while Discord remembers.
type savedState struct {
	Version int `json:"version"`
	// InvalidRequests pairs a unix second with the invalid responses recorded in it.
	InvalidRequests [][2]int64 `json:"invalidRequests,omitempty"`
	// EdgeBlockedUntil ends a confirmed Cloudflare block, in unix milliseconds.
	EdgeBlockedUntil int64          `json:"edgeBlockedUntil,omitempty"`
	Webhooks         []savedWebhook `json:"webhooks,omitempty"`
}

type savedWebhook struct {
	Key    string `json:"key"`
	Status int    `json:"status"`
	Code   int    `json:"code"`
	// Expires is in unix milliseconds.
	Expires int64 `json:"expires"`
}

func (p *Proxy) stateRevision() uint64 {
	return p.invalidRequests.revision.Load() + p.webhooks.revision.Load() + p.cloudflare.revision.Load()
}

func (p *Proxy) loadState() {
	data, err := readStateFile(p.config.StateFile)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	var state savedState
	if err == nil {
		err = json.Unmarshal(data, &state)
	}
	if err == nil && state.Version != stateVersion {
		err = fmt.Errorf("unknown format version %d", state.Version)
	}
	if err != nil {
		// The file may belong to something else; move it aside rather than overwrite it.
		aside := p.config.StateFile + ".invalid"
		moveErr := os.Rename(p.config.StateFile, aside)
		logger.Warn("Ignoring unreadable STATE_FILE; protection state starts empty", "file", p.config.StateFile, "error", err, "movedTo", aside, "moveError", moveErr)
		return
	}
	now := time.Now()
	blocked := false
	if p.config.CloudflareBanDetection {
		blocked = p.cloudflare.restore(time.UnixMilli(state.EdgeBlockedUntil), now)
	}
	logger.Info("Restored protection state",
		"file", p.config.StateFile,
		"invalidRequests", p.invalidRequests.restore(state.InvalidRequests, now),
		"cloudflareBlocked", blocked,
		"webhooks", p.webhooks.restore(state.Webhooks, now),
	)
}

func readStateFile(name string) ([]byte, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, maxStateFileBytes+1))
	if err == nil && len(data) > maxStateFileBytes {
		err = fmt.Errorf("larger than %d bytes", maxStateFileBytes)
	}
	return data, err
}

// saveState replaces the state file atomically, so a crash never leaves half of one.
func (p *Proxy) saveState() error {
	now := time.Now()
	state := savedState{
		Version:         stateVersion,
		InvalidRequests: p.invalidRequests.snapshot(now),
		Webhooks:        p.webhooks.snapshot(now),
	}
	if until := p.cloudflare.snapshot(now); !until.IsZero() {
		state.EdgeBlockedUntil = until.UnixMilli()
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	directory := filepath.Dir(p.config.StateFile)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(directory, ".sluice-state-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(temp.Name()) }()
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	return os.Rename(temp.Name(), p.config.StateFile)
}

func (p *Proxy) stateLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(stateSaveInterval)
	defer ticker.Stop()
	saved := p.stateRevision()
	failing := false
	for {
		select {
		case <-ticker.C:
			revision := p.stateRevision()
			if revision == saved {
				continue
			}
			if err := p.saveState(); err != nil {
				// Said once, when saving starts to fail: every try names a new temporary file, so
				// no two errors read the same.
				if !failing {
					failing = true
					logger.Warn("Could not save STATE_FILE; trying again every few seconds", "file", p.config.StateFile, "error", err)
				}
				continue
			}
			if failing {
				failing = false
				logger.Info("Saving STATE_FILE works again", "file", p.config.StateFile)
			}
			saved = revision
		case <-p.ctx.Done():
			return
		}
	}
}
