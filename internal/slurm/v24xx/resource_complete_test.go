package slurm_v24xx

import (
	"testing"

	"github.com/ClusterCockpit/cc-slurm-adapter/internal/slurm/common"
)

func TestResourceQuerySkipsCompleteJobs(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	id := int64(123)
	cluster := "example"
	job := &Job{sc: &ScontrolJob{JobId: &id, Cluster: &cluster}}
	api := slurmApi{}
	if err := api.QueryJobsWithResources(cluster, []slurm_common.Job{job}); err != nil {
		t.Fatal(err)
	}
}
