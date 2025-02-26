package main

import (
	"fmt"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
)

func NvidiaDetectMain() error {
	ret := nvml.Init()
	if ret != nvml.SUCCESS {
		return fmt.Errorf("Unable to intialize NVML: %s. Are any Nvidia GPUs installed and is the Nvidia driver installed?", nvml.ErrorString(ret))
	}

	defer nvml.Shutdown()

	count, ret := nvml.DeviceGetCount()
	if ret != nvml.SUCCESS {
		return fmt.Errorf("Unable to get device count: %s", nvml.ErrorString(ret))
	}

	if count == 0 {
		fmt.Println("Detect Nvidia driver, but no GPUs are available. Exiting.")
		return nil
	}

	fmt.Println("Detected GPUs:")

	for i := 0; i < count; i++ {
		device, ret := nvml.DeviceGetHandleByIndex(i)
		if ret != nvml.SUCCESS {
			return fmt.Errorf("Unable to get device at index %d: %s", i, nvml.ErrorString(ret))
		}

		pciInfo, ret := device.GetPciInfo()
		if ret != nvml.SUCCESS {
			return fmt.Errorf("Unable to get PCI address for device index %d: %s", i, nvml.ErrorString(ret))
		}

		name, ret := device.GetName()
		if ret != nvml.SUCCESS {
			return fmt.Errorf("Unable to get device name for device index %d: %s", i, nvml.ErrorString(ret))
		}

		busId := ""
		for _, v := range pciInfo.BusId {
			if v == 0 {
				break
			}
			busId += string(rune(v))
		}

		fmt.Printf("- [%d] %s (%s)\n", i, busId, name)
	}

	return nil
}
