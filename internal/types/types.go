package types

import (
	"github.com/ClusterCockpit/cc-lib/schema"
)

type CCStartJobRequest schema.Job

type CCStopJobRequest struct {
	JobId    uint32          `json:"jobId"     db:"job_id"`
	Cluster  string          `json:"cluster"   db:"cluster"`
	State    schema.JobState `json:"jobState"  db:"state"`
	StopTime int64           `json:"stopTime"  db:"stop_time"`
}

type CCNodeStat schema.NodePayload

type CCNodeStatRequest struct {
	Cluster string       `json:"cluster"`
	Nodes   []CCNodeStat `json:"nodes"`
}
