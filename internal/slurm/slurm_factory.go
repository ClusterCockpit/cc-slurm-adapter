package slurm

import (
	"fmt"

	"github.com/ClusterCockpit/cc-slurm-adapter/internal/slurm/v24xx"
	"github.com/ClusterCockpit/cc-slurm-adapter/internal/slurm/common"
)

// The interface below represents a generic interface around a specific Slurm version.
// This API should not change across Slurm versions, but it may require a new implementation.
// The version specific implementations can be found in the subdirectories 'vXXXX'.

func NewSlurmApi() (slurm_common.SlurmApi, error) {
	// v24 supports Slurm version v24.XX to v2X.XX TODO test with slurm v25.XX
	api, err_v24 := slurm_v24xx.NewSlurmApi()
	if err_v24 == nil {
		return api, nil
	}

	// TODO If we implement more Slurm versions, put them here.

	// If we have backends for other Slurm versions, perhaps return something more
	// useful so that the user knows if it's a version error or another type of error.
	return nil, fmt.Errorf("Unable to initialize Slurm API: %w", err_v24)
}
