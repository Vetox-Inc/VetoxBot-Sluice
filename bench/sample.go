package main

import (
	"bufio"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const failuresPrefix = `sluice_failures_total{reason="`

// usageSample is Sluice's footprint over one sampling interval.
type usageSample struct {
	Seconds    float64 `json:"seconds"`
	CPUCores   float64 `json:"cpuCores"`
	RSSBytes   float64 `json:"rssBytes"`
	Goroutines float64 `json:"goroutines"`
}

// sluiceUsage is what Sluice used during the measured part of a run, read from its own /metrics.
type sluiceUsage struct {
	IdleRSSBytes   float64            `json:"idleRssBytes"`
	CPUCoresMean   float64            `json:"cpuCoresMean"`
	RSSPeakBytes   float64            `json:"rssPeakBytes"`
	RSSEndBytes    float64            `json:"rssEndBytes"`
	GoroutinesPeak float64            `json:"goroutinesPeak"`
	GoroutinesEnd  float64            `json:"goroutinesEnd"`
	Failures       map[string]float64 `json:"failures"`
	Series         []usageSample      `json:"series,omitempty"`
}

type reading struct {
	at         time.Time
	cpuSeconds float64
	rssBytes   float64
	goroutines float64
	failures   map[string]float64
}

func scrape(client *http.Client, url string) (reading, error) {
	response, err := client.Get(url)
	if err != nil {
		return reading{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return reading{}, fmt.Errorf("metrics answered %d", response.StatusCode)
	}
	current := reading{at: time.Now(), failures: map[string]float64{}}
	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		space := strings.LastIndexByte(line, ' ')
		if space < 0 || line[0] == '#' {
			continue
		}
		value, err := strconv.ParseFloat(line[space+1:], 64)
		if err != nil {
			continue
		}
		switch name := line[:space]; {
		case name == "process_cpu_seconds_total":
			current.cpuSeconds = value
		case name == "process_resident_memory_bytes":
			current.rssBytes = value
		case name == "go_goroutines":
			current.goroutines = value
		case strings.HasPrefix(name, failuresPrefix):
			current.failures[strings.TrimSuffix(name[len(failuresPrefix):], `"}`)] = value
		}
	}
	return current, scanner.Err()
}

// usageOf sums up the readings of a run's measured part. A point of the series needs an interval
// long enough to measure: the reading taken when a run stops follows the one before it by a few
// milliseconds, and a CPU rate over so short a time is noise.
func usageOf(idle reading, measured []reading, interval time.Duration) *sluiceUsage {
	usage := &sluiceUsage{IdleRSSBytes: idle.rssBytes, Failures: map[string]float64{}}
	if len(measured) == 0 {
		return usage
	}
	first, last := measured[0], measured[len(measured)-1]
	if elapsed := last.at.Sub(first.at).Seconds(); elapsed > 0 {
		usage.CPUCoresMean = (last.cpuSeconds - first.cpuSeconds) / elapsed
	}
	usage.RSSEndBytes, usage.GoroutinesEnd = last.rssBytes, last.goroutines
	for reason, count := range last.failures {
		if count > 0 {
			usage.Failures[reason] = count
		}
	}
	// A long run keeps about a hundred points of its series, which is enough to show a trend.
	stride := max(1, len(measured)/100)
	for i, current := range measured {
		usage.RSSPeakBytes = max(usage.RSSPeakBytes, current.rssBytes)
		usage.GoroutinesPeak = max(usage.GoroutinesPeak, current.goroutines)
		if i == 0 || i%stride != 0 {
			continue
		}
		previous := measured[i-1]
		elapsed := current.at.Sub(previous.at)
		if elapsed < interval/2 {
			continue
		}
		usage.Series = append(usage.Series, usageSample{
			Seconds:    current.at.Sub(first.at).Seconds(),
			CPUCores:   (current.cpuSeconds - previous.cpuSeconds) / elapsed.Seconds(),
			RSSBytes:   current.rssBytes,
			Goroutines: current.goroutines,
		})
	}
	return usage
}

// sampler reads Sluice's process metrics at an interval for as long as a run lasts.
type sampler struct {
	url      string
	interval time.Duration
	client   *http.Client
	quit     chan struct{}
	finished chan struct{}

	mu         sync.Mutex
	idle       reading
	readings   []reading
	markedFrom int
}

func startSampler(url string, interval time.Duration) (*sampler, error) {
	probe := &sampler{
		url:      url,
		interval: interval,
		client:   &http.Client{Timeout: 5 * time.Second},
		quit:     make(chan struct{}),
		finished: make(chan struct{}),
	}
	idle, err := scrape(probe.client, url)
	if err != nil {
		return nil, err
	}
	probe.idle = idle
	go func() {
		defer close(probe.finished)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-probe.quit:
				return
			case <-ticker.C:
				probe.read()
			}
		}
	}()
	return probe, nil
}

func (s *sampler) read() {
	current, err := scrape(s.client, s.url)
	if err != nil {
		return
	}
	s.mu.Lock()
	s.readings = append(s.readings, current)
	s.mu.Unlock()
}

// mark starts the measured part of the run: readings before it are warm-up.
func (s *sampler) mark() {
	s.read()
	s.mu.Lock()
	s.markedFrom = max(len(s.readings)-1, 0)
	s.mu.Unlock()
}

func (s *sampler) stop() *sluiceUsage {
	close(s.quit)
	<-s.finished
	s.read()

	s.mu.Lock()
	defer s.mu.Unlock()
	return usageOf(s.idle, s.readings[min(s.markedFrom, len(s.readings)):], s.interval)
}
