package slurm_common

import (
	"time"

	"github.com/ClusterCockpit/cc-slurm-adapter/internal/types"

	"github.com/ClusterCockpit/cc-lib/schema"
)

type Job interface {
	// TODO document, what those do exactly
	GetJobId() int64
	GetCluster() string
	GetPartition() string
	GetName() string
	GetUser() string
	GetGroup() string
	GetAccount() string

	GetState() string
	IsFinished() bool
	GetSubmitTime() time.Time
	GetStartTime() time.Time
	GetEndTime() time.Time
	GetTimeLimit() time.Duration

	GetResources() ([]*schema.Resource, error)
	GetJobScript() string
	GetSlurmInfo() string

	GetArrayJobId() int64
	GetNumNodes() int32
	GetNumHWThreads() int32
	GetNumAccelerators() int32
	GetNodeShared() string

	// TODO, perhaps give those below more meaningful names
	HasResourceInfo() bool
	HasDbInfo() bool
}

type SlurmApi interface {
	// Wrapper around sacctmgr to list the clusters
	GetClusterNames() []string

	// Wrapper around sacct for single job
	QueryJobs(clusterName string, jobId []int64) ([]Job, error)

	// Wrapper around sacct with time range
	QueryJobsTimeRange(clusterName string, begin, end time.Time) ([]Job, error)

	// Wrapper around squeue
	QueryJobsActive(clusterName string) ([]Job, error)

	// Wrapper around squeue for existing job list
	QueryJobsWithResources(clusterName string, jobs []Job) error

	// Wrapper around sinfo
	QueryNodeStats(clusterName string) ([]types.CCNodeStat, error)
}
