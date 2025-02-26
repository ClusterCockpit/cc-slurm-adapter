package main

import (
	"flag"
	"os"

	"github.com/ClusterCockpit/cc-slurm-adapter/trace"
)

func main() {
	var help bool
	flag.BoolVar(&help, "help", false, "Print command line help")

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

	if help {
		flag.PrintDefaults()
		os.Exit(0)
	} else if nvidiaDetect {
		mode = "Nvidia Detect"
		err = NvidiaDetectMain()
	} else if daemon {
		mode = "Daemon"
		err = DaemonMain()
	} else {
		mode = "Prolog/Epilog"
		err = PrologEpilogMain()
	}

	if err != nil {
		trace.Error("Main function encountered an error: %v", err)
		os.Exit(1)
	}

	trace.Info("cc-slurm-adapter (%s) terminated sucessfully", mode)
}
