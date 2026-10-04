// Command bench measures Sluice against a mock Discord: the same workload is sent straight to
// the mock and sent through Sluice. BENCHMARKS.md explains the method and run.sh drives it.
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "mock":
		err = runMock(os.Args[2:])
	case "load":
		err = runLoad(os.Args[2:])
	case "report":
		err = runReport(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: bench mock|load|report [flags]")
	os.Exit(2)
}
