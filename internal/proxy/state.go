package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

const (
	stateVersion      = 1
	stateSaveInterval = 5 * time.Second
	// maxStateFileBytes is far above a full state, about 1 MiB with every webhook slot in use.
	maxStateFileBytes = 16 << 20
	stateTempPattern  = ".sluice-state-*"
	// staleStateTempAge is how old a temp file must be before startup removes it: a younger one
	// may belong to another instance saving into the same directory.
	staleStateTempAge = time.Minute
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
	// RefusalGuardUntil ends the refusal guard's pressure after a trip, in unix milliseconds. Like
	// any field added since 1.0.0 it is optional, so 1.0.0 still reads the file.
	RefusalGuardUntil int64 `json:"refusalGuardUntil,omitempty"`
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
	removeStaleStateTemps(p.config.StateFile, time.Now())
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
		"refusalGuard", p.refusals.restore(time.UnixMilli(state.RefusalGuardUntil), now),
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
	if until := p.refusals.snapshot(now); !until.IsZero() {
		state.RefusalGuardUntil = until.UnixMilli()
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	directory := filepath.Dir(p.config.StateFile)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(directory, stateTempPattern)
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
	if err := os.Rename(temp.Name(), p.config.StateFile); err != nil {
		return err
	}
	p.syncStateDirectory(directory)
	return nil
}

// syncStateDirectory makes the rename itself durable, so a power loss cannot bring back the
// previous file. It never fails a save, and says so only once; Windows cannot sync a directory.
func (p *Proxy) syncStateDirectory(directory string) {
	if runtime.GOOS == "windows" {
		return
	}
	dir, err := os.Open(directory)
	if err == nil {
		err = dir.Sync()
		if closeErr := dir.Close(); err == nil {
			err = closeErr
		}
	}
	if err != nil && p.directorySyncWarned.CompareAndSwap(false, true) {
		logger.Warn("Could not sync STATE_FILE's directory; the file is saved, but a power loss may undo the last save", "directory", directory, "error", err)
	}
}

// removeStaleStateTemps deletes temp files a process that died mid-save left next to STATE_FILE.
func removeStaleStateTemps(stateFile string, now time.Time) {
	names, _ := filepath.Glob(filepath.Join(filepath.Dir(stateFile), stateTempPattern))
	for _, name := range names {
		if info, err := os.Lstat(name); err == nil && info.Mode().IsRegular() && now.Sub(info.ModTime()) > staleStateTempAge {
			_ = os.Remove(name)
		}
	}
}

// saveStateSoon asks stateLoop to save at once rather than at its next tick: a pause starting or
// changing, a step of invalid responses or a refusal-guard trip must survive a crash.
func (p *Proxy) saveStateSoon() {
	select {
	case p.saveRequests <- struct{}{}:
	default:
	}
}

func (p *Proxy) stateLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(stateSaveInterval)
	defer ticker.Stop()
	saved := p.stateRevision()
	failing := false
	for {
		urgent := false
		select {
		case <-ticker.C:
		case <-p.saveRequests:
			urgent = true
		case <-p.ctx.Done():
			return
		}
		revision := p.stateRevision()
		if revision == saved && !urgent {
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
	}
}
