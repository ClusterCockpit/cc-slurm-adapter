package main

import (
	"flag"
	"os"
	"fmt"

	"github.com/ClusterCockpit/cc-slurm-adapter/trace"
)

func main() {
	var prolog bool
	flag.BoolVar(&prolog, "prolog", false, "Submit a job from a Slurm prolog script context to the running cc-slurm-adapter.")

	var epilog bool
	flag.BoolVar(&epilog, "epilog", false, "Submit a job from a Slurm epilog script context to the running cc-slurm-adapter.")

	var daemon bool
	flag.BoolVar(&daemon, "daemon", false, "Start cc-slurm-adapter daemon. Prolog and Epilog calls require a running daemon.")

	var nvidiaDetect bool
	flag.BoolVar(&nvidiaDetect, "nvidia-detect", false, "Detect Nvidia GPUs in this system for use in the accelerator config.")

	var debugLevel int
	flag.IntVar(&debugLevel, "debug", 2, "Set log level")

	var configPath string
	flag.StringVar(&configPath, "config", "", "Specify configuration file path")

	flag.Parse()

	trace.SetLevel(debugLevel)
	LoadConfig(configPath)

	var err error
	var mode string

	if nvidiaDetect {
		mode = "Nvidia Detect"
		err = NvidiaDetectMain()
	} else if prolog && epilog {
		err = fmt.Errorf("Prolog and Epilog must not be used at the same time")
	} else if prolog {
		mode = "Prolog"
		err = PrologEpilogMain()
	} else if epilog {
		mode = "Epilog"
		err = PrologEpilogMain()
	} else if daemon {
		mode = "Daemon"
		err = DaemonMain()
	} else {
		flag.PrintDefaults()
		os.Exit(0)
	}

	if err != nil {
		trace.Error("Main function encountered an error: %v", err)
		os.Exit(1)
	}

	trace.Info("cc-slurm-adapter (%s) terminated sucessfully", mode)
}
