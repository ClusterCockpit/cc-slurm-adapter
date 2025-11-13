package types

import (
	"github.com/ClusterCockpit/cc-lib/schema"
)

// The types kind of don't belong here, since they are ClusterCockpit types.
// However, we cannot use the 'cc' packages from the 'slurm' pacakge due to
// cyclic dependency. Maybe we find a nicer solution in the future.
type CCStartJobRequest schema.Job

type CCStopJobRequest struct {
	JobId    uint32          `json:"jobId"     db:"job_id"`
	Cluster  string          `json:"cluster"   db:"cluster"`
	State    schema.JobState `json:"jobState"  db:"state"`
	StopTime int64           `json:"stopTime"  db:"stop_time"`
}
