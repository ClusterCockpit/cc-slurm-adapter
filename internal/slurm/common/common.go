package slurm_common

import (
	"time"

	"github.com/ClusterCockpit/cc-slurm-adapter/internal/types"
)

type SacctJob interface {
	GetJobId() int64
	GetCluster() string
	GetState() string
	IsFinished() bool
}

type ScontrolJob interface {
	GetJobId() int64
	GetCluster() string
	GetState() string
	IsRunning() bool
}

type SlurmApi interface {
	// Wrapper around sacctmgr to list the clusters
	GetClusterNames() []string

	// Wrapper around sacct for single job
	QueryJob(clusterName string, jobId uint32) (SacctJob, error)

	// Wrapper around sacct with time range
	QueryJobsTimeRange(clusterName string, begin, end time.Time) ([]SacctJob, error)

	// Wrapper around squeue
	QueryJobsActive(clusterName string) ([]ScontrolJob, error)

	// Wrapper around sinfo
	QueryNodeStats(clusterName string) ([]types.CCNodeStat, error)

	// Whenever QueryJob is called, the job is cached. This function resets it
	ClearJobCache()

	// Convert a job retrieved via QueryJob to a ClusterCockpit start_job request
	JobToCCStartJob(job SacctJob) (*types.CCStartJobRequest, error)

	// Convert a job retrieved via QueryJob to a ClusterCockpit stop_job request
	JobToCCStopJob(job SacctJob) (*types.CCStopJobRequest, error)
}
