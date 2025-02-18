package main

import (
	"flag"
	"os"
)

func main() {
	var prolog bool
	flag.BoolVar(&prolog, "prolog", false, "Submit a job from a Slurm prolog script context to the running cc-slurm-adapter.")

	var epilog bool
	flag.BoolVar(&epilog, "epilog", false, "Submit a job from a Slurm epilog script context to the running cc-slurm-adapter.")

	var daemon bool
	flag.BoolVar(&daemon, "daemon", false, "Start cc-slurm-adapter daemon. Prolog and Epilog calls require a running daemon.")

	var debugLevel int
	flag.IntVar(&debugLevel, "debug", 0, "Set debug level")

	flag.Parse()

	if prolog && epilog {
		println("Prolog and Epilog must not be used at the same time")
		os.Exit(1)
	} else if prolog {
		PrologMain()
	} else if epilog {
		EpilogMain()
	} else if daemon {
		DaemonMain()
	} else {
		flag.PrintDefaults()
		os.Exit(0)
	}
}
