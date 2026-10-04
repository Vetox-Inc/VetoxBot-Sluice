package main

import (
	"math"
	"slices"
	"time"
)

// latencySummary describes a set of latencies, in microseconds.
type latencySummary struct {
	Count int     `json:"count"`
	Min   float64 `json:"min"`
	Mean  float64 `json:"mean"`
	P50   float64 `json:"p50"`
	P90   float64 `json:"p90"`
	P99   float64 `json:"p99"`
	P999  float64 `json:"p999"`
	Max   float64 `json:"max"`
}

func micros(duration time.Duration) float64 {
	return float64(duration.Nanoseconds()) / 1000
}

// summarize sorts samples in place.
func summarize(samples []time.Duration) latencySummary {
	if len(samples) == 0 {
		return latencySummary{}
	}
	slices.Sort(samples)
	var total time.Duration
	for _, sample := range samples {
		total += sample
	}
	return latencySummary{
		Count: len(samples),
		Min:   micros(samples[0]),
		Mean:  micros(total / time.Duration(len(samples))),
		P50:   micros(percentile(samples, 50)),
		P90:   micros(percentile(samples, 90)),
		P99:   micros(percentile(samples, 99)),
		P999:  micros(percentile(samples, 99.9)),
		Max:   micros(samples[len(samples)-1]),
	}
}

// percentile is the nearest-rank percentile of sorted samples.
func percentile(sorted []time.Duration, rank float64) time.Duration {
	index := int(math.Ceil(rank / 100 * float64(len(sorted))))
	return sorted[max(index, 1)-1]
}

// hostInfo records where a run happened, and how busy that machine was around it.
type hostInfo struct {
	CPUModel   string `json:"cpuModel"`
	Kernel     string `json:"kernel"`
	CPUs       int    `json:"cpus"`
	LoadBefore string `json:"loadBefore"`
	LoadAfter  string `json:"loadAfter"`
}

// result is one run of one scenario over one path, as written to its JSON file.
type result struct {
	Scenario   string            `json:"scenario"`
	Kind       string            `json:"kind"`
	Via        string            `json:"via"`
	Rep        int               `json:"rep"`
	SUT        string            `json:"sut,omitempty"`
	StartedAt  time.Time         `json:"startedAt"`
	Parameters map[string]string `json:"parameters"`

	// Requests, Failed, Throughput and Latency cover the measured window only; the rest cover
	// the whole run, warm-up included, so they can be checked against the mock's own counts.
	MeasuredSeconds float64        `json:"measuredSeconds"`
	Requests        int            `json:"requests"`
	Failed          int            `json:"failed"`
	Throughput      float64        `json:"throughput"`
	Latency         latencySummary `json:"latencyMicros"`
	// GeneratorLagP99 is how late the load generator itself sent a request, at worst: a large
	// value means the generator, not the system measured, fell behind.
	GeneratorLagP99 float64 `json:"generatorLagP99Micros,omitempty"`

	Attempts        int            `json:"attempts"`
	Succeeded       int            `json:"succeeded"`
	Responses       map[string]int `json:"responses"`
	TransportErrors int            `json:"transportErrors"`
	Unfinished      int            `json:"unfinished,omitempty"`
	// LimitedBySluice is the 429s Sluice answered itself during the measured part.
	LimitedBySluice int `json:"limitedBySluice"`

	// Mock is what the mock counted over the whole run, and MockMeasured over the measured part.
	Mock         mockStats    `json:"mock"`
	MockMeasured *mockStats   `json:"mockMeasured,omitempty"`
	Sluice       *sluiceUsage `json:"sluice,omitempty"`
	Host         hostInfo     `json:"host"`
	Reconcile    string       `json:"reconcile"`
}
