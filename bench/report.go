package main

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

var paths = []string{"direct", "sluice"}

func formatMicros(value float64) string {
	switch {
	case value < 1000:
		return fmt.Sprintf("%.0f µs", value)
	case value < 1_000_000:
		return fmt.Sprintf("%.2f ms", value/1000)
	}
	return fmt.Sprintf("%.2f s", value/1_000_000)
}

func formatMiB(bytes float64) string {
	return fmt.Sprintf("%.1f MiB", bytes/(1<<20))
}

func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := slices.Sorted(slices.Values(values))
	middle := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[middle]
	}
	return (sorted[middle-1] + sorted[middle]) / 2
}

// across is the median of one measurement over the repetitions of a scenario.
func across(runs []result, measure func(result) float64) float64 {
	values := make([]float64, len(runs))
	for i, run := range runs {
		values[i] = measure(run)
	}
	return median(values)
}

func total(runs []result, measure func(result) float64) float64 {
	sum := 0.0
	for _, run := range runs {
		sum += measure(run)
	}
	return sum
}

// seenByDiscord is what the mock counted during the measured part of a run.
func seenByDiscord(run result) mockStats {
	if run.MockMeasured != nil {
		return *run.MockMeasured
	}
	return run.Mock
}

func limitedByDiscord(run result) float64 {
	seen := seenByDiscord(run)
	return float64(seen.LimitedGlobal + seen.LimitedRoute)
}

func limitedBySluice(run result) float64 {
	return float64(run.LimitedBySluice)
}

func reachedDiscord(run result) float64 {
	return float64(seenByDiscord(run).Received)
}

func failed(run result) float64 {
	return float64(run.Failed + run.Unfinished)
}

// footprint formats what Sluice used, or a dash for the direct path, which has no proxy.
func footprint(runs []result) (cpu, memory string) {
	if len(runs) == 0 || runs[0].Sluice == nil {
		return "-", "-"
	}
	cores := across(runs, func(run result) float64 { return run.Sluice.CPUCoresMean })
	peak := across(runs, func(run result) float64 { return run.Sluice.RSSPeakBytes })
	return fmt.Sprintf("%.2f cores", cores), formatMiB(peak)
}

// oneMinute is the one-minute figure of a load average.
func oneMinute(load string) string {
	first, _, _ := strings.Cut(load, " ")
	return first
}

func loadResults(directory string) ([]result, error) {
	files, err := filepath.Glob(filepath.Join(directory, "*.json"))
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no results in %s", directory)
	}
	runs := make([]result, 0, len(files))
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, err
		}
		var run result
		if err := json.Unmarshal(data, &run); err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		runs = append(runs, run)
	}
	slices.SortFunc(runs, func(a, b result) int {
		return cmp.Or(cmp.Compare(a.Scenario, b.Scenario), cmp.Compare(a.Via, b.Via), cmp.Compare(a.Rep, b.Rep))
	})
	return runs, nil
}

// scenarios lists the scenario names of the given kinds, in order.
func scenarios(runs []result, kinds ...string) []string {
	var names []string
	for _, run := range runs {
		if slices.Contains(kinds, run.Kind) && !slices.Contains(names, run.Scenario) {
			names = append(names, run.Scenario)
		}
	}
	return names
}

func of(runs []result, scenario, via string) []result {
	var matching []result
	for _, run := range runs {
		if run.Scenario == scenario && run.Via == via {
			matching = append(matching, run)
		}
	}
	return matching
}

func latency(pick func(latencySummary) float64) func(result) float64 {
	return func(run result) float64 { return pick(run.Latency) }
}

var (
	p50  = latency(func(l latencySummary) float64 { return l.P50 })
	p90  = latency(func(l latencySummary) float64 { return l.P90 })
	p99  = latency(func(l latencySummary) float64 { return l.P99 })
	p999 = latency(func(l latencySummary) float64 { return l.P999 })
	top  = latency(func(l latencySummary) float64 { return l.Max })
)

func throughputTable(runs []result, kinds ...string) {
	fmt.Println("| Scenario | Path | Requests/s | p50 | p90 | p99 | p99.9 | Max | Failed | Sluice CPU | Sluice memory |")
	fmt.Println("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
	for _, scenario := range scenarios(runs, kinds...) {
		for _, via := range paths {
			group := of(runs, scenario, via)
			if len(group) == 0 {
				continue
			}
			cpu, memory := footprint(group)
			fmt.Printf("| %s | %s | %.0f | %s | %s | %s | %s | %s | %.0f | %s | %s |\n", scenario, via,
				across(group, func(run result) float64 { return run.Throughput }),
				formatMicros(across(group, p50)), formatMicros(across(group, p90)), formatMicros(across(group, p99)),
				formatMicros(across(group, p999)), formatMicros(across(group, top)), total(group, failed), cpu, memory)
		}
		direct, sluice := of(runs, scenario, "direct"), of(runs, scenario, "sluice")
		if len(direct) == 0 || len(sluice) == 0 {
			continue
		}
		added := func(measure func(result) float64) string {
			return formatMicros(max(across(sluice, measure)-across(direct, measure), 0))
		}
		fmt.Printf("| %s | added by Sluice | | %s | %s | %s | %s | | | | |\n", scenario, added(p50), added(p90), added(p99), added(p999))
	}
	fmt.Println()
}

func limitsTable(runs []result, kinds ...string) {
	fmt.Println("| Scenario | Path | Completed | Failed | Time | 429s from Discord | 429s from Sluice | Reached Discord | p50 | p99 | Max | Sluice CPU | Sluice memory |")
	fmt.Println("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
	for _, scenario := range scenarios(runs, kinds...) {
		for _, via := range paths {
			group := of(runs, scenario, via)
			if len(group) == 0 {
				continue
			}
			cpu, memory := footprint(group)
			fmt.Printf("| %s | %s | %.0f | %.0f | %.1f s | %.0f | %.0f | %.0f | %s | %s | %s | %s | %s |\n", scenario, via,
				across(group, func(run result) float64 { return float64(run.Requests) }), total(group, failed),
				across(group, func(run result) float64 { return run.MeasuredSeconds }),
				across(group, limitedByDiscord), across(group, limitedBySluice), across(group, reachedDiscord),
				formatMicros(across(group, p50)), formatMicros(across(group, p99)), formatMicros(across(group, top)), cpu, memory)
		}
	}
	fmt.Println()
}

// trend prints how Sluice's footprint moved over a long run.
func trend(runs []result) {
	for _, run := range runs {
		if run.Kind != "mixed" || run.Sluice == nil || len(run.Sluice.Series) == 0 {
			continue
		}
		fmt.Printf("Sluice during `%s`, run %d (idle before the run: %s):\n\n", run.Scenario, run.Rep, formatMiB(run.Sluice.IdleRSSBytes))
		fmt.Println("| Minute | Memory | Goroutines | CPU |")
		fmt.Println("| ---: | ---: | ---: | ---: |")
		points := run.Sluice.Series
		step := max(1, len(points)/12)
		for i := 0; i < len(points); i += step {
			fmt.Printf("| %.1f | %s | %.0f | %.3f cores |\n", points[i].Seconds/60, formatMiB(points[i].RSSBytes), points[i].Goroutines, points[i].CPUCores)
		}
		if last := points[len(points)-1]; (len(points)-1)%step != 0 {
			fmt.Printf("| %.1f | %s | %.0f | %.3f cores |\n", last.Seconds/60, formatMiB(last.RSSBytes), last.Goroutines, last.CPUCores)
		}
		fmt.Println()
	}
}

func everyRun(runs []result) {
	fmt.Println("| Scenario | Path | Run | Started (UTC) | Requests | Failed | Requests/s | p50 | p99 | Max | 429s from Discord | 429s from Sluice | Reached Discord | Sluice CPU | Sluice memory | Load before, after | Check |")
	fmt.Println("| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |")
	for _, run := range runs {
		cpu, memory := footprint([]result{run})
		fmt.Printf("| %s | %s | %d | %s | %d | %.0f | %.1f | %s | %s | %s | %.0f | %.0f | %.0f | %s | %s | %s, %s | %s |\n",
			run.Scenario, run.Via, run.Rep, run.StartedAt.Format("15:04:05"), run.Requests, failed(run), run.Throughput,
			formatMicros(run.Latency.P50), formatMicros(run.Latency.P99), formatMicros(run.Latency.Max),
			limitedByDiscord(run), limitedBySluice(run), reachedDiscord(run), cpu, memory,
			oneMinute(run.Host.LoadBefore), oneMinute(run.Host.LoadAfter), run.Reconcile)
	}
	fmt.Println()
}

// runReport prints the tables of BENCHMARKS.md from a directory of results.
func runReport(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: bench report <results directory>")
	}
	runs, err := loadResults(args[0])
	if err != nil {
		return err
	}
	first := runs[0]
	fmt.Printf("Measured: %s. Host: %s, kernel %s. Runs: %d, from %s.\n\n",
		first.SUT, first.Host.CPUModel, first.Host.Kernel, len(runs), first.StartedAt.Format("2006-01-02"))
	fmt.Print("### Requests at a fixed rate\n\n")
	throughputTable(runs, "rate")
	fmt.Print("### Every connection kept busy\n\n")
	throughputTable(runs, "closed")
	fmt.Print("### Work that meets Discord's limits\n\n")
	limitsTable(runs, "batch", "mixed")
	trend(runs)
	fmt.Print("### Every run\n\n")
	everyRun(runs)
	return nil
}
