package main

import (
	"time"
	"bytes"
	"os/exec"
	"encoding/json"
	"fmt"
	"strings"
	"strconv"

	"github.com/ClusterCockpit/cc-slurm-adapter/trace"
	"github.com/ClusterCockpit/cc-backend/pkg/schema"
)

/* SlurmInt supports these two JSON layouts:
 * - 42
 * - { "set": true, "infinite": false, "number": 42 }
 */
type SlurmInt struct {
	Set bool `json:"set"`
	Infinite bool `json:"infinite"`
	Number int64 `json:"number"`
}

/* SlurmString supports these two JSON layouts:
 * - "myString"
 * - [ "myString" ]
 */
type SlurmString string

type ScontrolJobResourcesNodesAllocationSocketCore struct {
	Index *int `json:"index"`
	Status *SlurmString `json:"status"`
}

type ScontrolJobResourcesNodesAllocationSocket struct {
	Index *int `json:"index"`
	Cores []ScontrolJobResourcesNodesAllocationSocketCore `json:"cores"`
}

type ScontrolJobResourcesNodesAllocation struct {
	Hostname *string `json:"name"`
	Sockets []ScontrolJobResourcesNodesAllocationSocket `json:"sockets"`
}

type ScontrolJobResourcesNodes struct {
	Allocation []ScontrolJobResourcesNodesAllocation `json:"allocation"`
}

type ScontrolJobResources struct {
	Nodes *ScontrolJobResourcesNodes `json:"nodes"`
	CPUs *SlurmInt `json:"cpus"`
	ThreadsPerCore *SlurmInt `json:"threads_per_core"`
}

type ScontrolJob struct {
	/* Only (our) required fields are listed here. */
	JobId *uint32 `json:"job_id"`
	JobResources *ScontrolJobResources `json:"job_resources"`
	Comment *string `json:"comment"`
}

type ScontrolResult struct {
	Jobs []ScontrolJob `json:"jobs"`
	Meta *SlurmMeta `json:"meta"`
}

type SacctJobState struct {
	Current *SlurmString `json:"current"`
}

type SacctJob struct {
	/* Only (our) required fields are listed here. */
	Account *string `json:"account"`
	AllocationNodes *SlurmInt `json:"allocation_nodes"`
	ArrayJobId *SlurmInt `json:"array_job_id"`
	Cluster *string `json:"cluster"`
	JobId *uint32 `json:"job_id"`
	Name *string `json:"name"`
	Partition *string `json:"partition"`
	Required *SacctJobRequired `json:"required"`
	State *SacctJobState `json:"state"`
	Time *SacctJobTime `json:"time"`
	Shared *SlurmString `json:"shared"`
	Exclusive *SlurmString `json:"exclusive"`
	Script *string `json:"script"`
}

type SacctJobRequired struct {
	CPUs *SlurmInt `json:"CPUs"`
	MemoryPerCPU *SlurmInt `json:"memory_per_cpu"`
	MemoryPerNode *SlurmInt `json:"memory_per_node"`
}

type SacctJobTime struct {
	/* Only (our) required fields are listed here. */
	Elapsed SlurmInt `json:"elapsed"`
	End SlurmInt `json:"end"`
	Limit SlurmInt `json:"limit"`
	Start SlurmInt `json:"start"`
}
	
type SlurmMetaSlurmVersion struct {
	Major string `json:"major"`
	Minor string `json:"minor"`
	Micro string `json:"micro"`
}

type SlurmMetaSlurm struct {
	Version SlurmMetaSlurmVersion `json:"version"`
	Release string `json:"release"`
	Cluster string `json:"cluster"`
}

type SlurmMeta struct {
	/* Only (our) required fields are listed here. */
	Slurm SlurmMetaSlurm `json:"slurm"`
}

type SacctResult struct {
	/* Only (our) required fields are listed here. */
	Jobs []SacctJob `json:"jobs"`
	Meta SlurmMeta `json:"meta"`
}

const (
	SLURM_VERSION_INCOMPATIBLE string = "Unable to parse sacct JSON. Is cc-slurm-adapter compatible with this Slurm version?"
	SLURM_MAX_VER_MAJ int = 24
	SLURM_MAX_VER_MIN int = 11
)

func (v *SlurmInt) UnmarshalJSON(data []byte) error {
	/* Slurm at some point has changed the representation of integers in its API.
	 * Unfortuantely the usage is somewhat mixed, so we use a custom integer type
	 * with our own Unmarshal and Marshal functions. That way we can automatically
	 * switch between the two variants and simply use "SlurmInt" as type in the structs
	 * regardless of the Slurm version used. */
	result := struct{
		Set *bool `json:"set"`
		Infinite *bool `json:"finite"`
		Number *int64 `json:"number"`
	}{}
	err := json.Unmarshal(data, &result)
	if err == nil {
		if result.Set != nil && result.Infinite != nil && result.Number != nil {
			*v = SlurmInt{
				Set: *result.Set,
				Infinite: *result.Infinite,
				Number: *result.Number,
			}
			return nil
		}
	}

	var result2 int64
	err = json.Unmarshal(data, &result)
	if err == nil {
		*v = SlurmInt{
			Set: true,
			Infinite: false,
			Number: result2,
		}
		return nil
	}

	return fmt.Errorf("Unable to parse '%v' as Slurm legacy integer nor new integer")
}

func (v *SlurmString) UnmarshalJSON(data []byte) error {
	/* Slurm at some point wrapped strings in a list with just one string.
	 * No idea why. */
	var result []string
	err := json.Unmarshal(data, &result)
	if err == nil {
		if len(result) == 0 {
			*v = ""
		} else {
			*v = SlurmString(result[0])
		}
		return nil
	}

	*v = SlurmString(data)
	return nil
}

func SlurmQueryJob(jobId uint32) (*SacctJob, error) {
	stdout, err := callProcess("sacct", "-j", string(jobId), "--json")
	if err != nil {
		return nil, fmt.Errorf("Unable to run sacct: %w", err)
	}

	var result SacctResult
	err = json.Unmarshal([]byte(stdout), &result)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", SLURM_VERSION_INCOMPATIBLE, err)
	}

	SlurmWarnVersion(result.Meta.Slurm.Version)

	if len(result.Jobs) == 0 {
		return nil, fmt.Errorf("Requested job (%d) unavailable", jobId)
	}

	if len(result.Jobs) >= 2 {
		return nil, fmt.Errorf("Received more than one job for job %d", jobId)
	}

	return &result.Jobs[0], nil
}

func SlurmQueryJobsTimeRange(begin time.Time, end time.Time) ([]SacctJob, error) {
	starttime := begin.Format(time.DateTime) // e.g. '2025-02-24 15:00'
	endtime := end.Format(time.DateTime) // e.g. '2025-02-24 15:00'
	stdout, err := callProcess("sacct", "--starttime", starttime, "--endtime", endtime, "--json")
	if err != nil {
		return nil, fmt.Errorf("Unable to run sacct: %w", err)
	}

	var result SacctResult
	err = json.Unmarshal([]byte(stdout), &result)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", SLURM_VERSION_INCOMPATIBLE, err)
	}

	SlurmWarnVersion(result.Meta.Slurm.Version)
	return result.Jobs, nil
}

func SlurmQueryJobsActive() ([]SacctJob, error) {
	stdout, err := callProcess("squeue", "--json")
	if err != nil {
		return nil, fmt.Errorf("Unable to run sacct: %w", err)
	}

	var result SacctResult
	err = json.Unmarshal([]byte(stdout), &result)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", SLURM_VERSION_INCOMPATIBLE, err)
	}

	SlurmWarnVersion(result.Meta.Slurm.Version)
	return result.Jobs, nil
}

func SlurmGetResources(job SacctJob) ([]*schema.Resource, error) {
	/* This function fetches additional information about a Slurm job via scontrol.
	 * Unfortunately some of the information is not available via sacct, so we need
	 * scontrol to get this information. Because this information is not stored
	 * in the slurmdbd, we have to query this within a few minutes after a job has
	 * terminated at last.
	 * If this fetching fails, we cannot populate allocated resources. This is not
	 * critical to the operation of cc-backend, but it means certains graphs won't be
	 * available, since metrics won't be assignable to a job anymore. */
	stdout, err := callProcess("scontrol", "show", "job", string(*job.JobId), "--json")
	if err != nil {
		return nil, err
	}

	var scResult ScontrolResult
	err = json.Unmarshal([]byte(stdout), &scResult)
	if err != nil {
		return nil, err
	}

	/* Create schema.Resources out of the ScontrolResult */
	if len(scResult.Jobs) == 0 {
		/* If no jobs are returned, this is most likely because the job has already ended some time ago.
		 * There is nothing we can do about this, so continue with just a warning. */
		trace.Warn("Unable to get job resources, continuing without resources: %s", err)
		return make([]*schema.Resource, 0), nil
	}

	if len(scResult.Jobs) >= 1 {
		return nil, fmt.Errorf("'scontrol show job %d' returned too many jobs (%d > 1)", len(scResult.Jobs))
	}

	scJob := scResult.Jobs[0]
	scAllocation := scJob.JobResources.Nodes.Allocation
	resources := make([]*schema.Resource, 0)
	for _, allocation := range scAllocation {
		/* Determine Hwthreads */
		hwthreads := make([]int, 0)
		cpusPerSocket := len(allocation.Sockets[0].Cores)
		for _, socket := range allocation.Sockets {
			for _, core := range socket.Cores {
				if string(*core.Status) != "ALLOCATED" {
					continue
				}
				hwthreads = append(hwthreads, *socket.Index * cpusPerSocket + *core.Index)
			}
		}

		/* Determine accelerators. Currently this is only possible via the comment field */
		accelerators := strings.Split(*scJob.Comment, ",")

		/* Create final result */
		r := schema.Resource{
			Hostname: *allocation.Hostname,
			HWThreads: hwthreads,
			Accelerators: accelerators,
		}
		resources = append(resources, &r)
	}

	return resources, nil
}

func SlurmGetJobInfoText(job SacctJob) string {
	stdout, err := callProcess("scontrol", "show", "job", string(*job.JobId))
	if err != nil {
		/* If query fails, this is most likely because the job has already ended some time ago.
		 * There is nothing we can do about this, so continue with just a warning. */
		return fmt.Sprintf("Error while getting job information for JobID=%d", job.JobId)
	}

	return strings.TrimSpace(stdout)
}

func SlurmWarnVersion(ver SlurmMetaSlurmVersion) {
	major, _ := strconv.Atoi(ver.Major)
	minor, _ := strconv.Atoi(ver.Minor)
	if major < SLURM_MAX_VER_MAJ {
		return
	}
	if major == SLURM_MAX_VER_MAJ && minor < SLURM_MAX_VER_MIN {
		return
	}
	trace.Warn("Detected Slurm version %s.%s.%s. Last supported version is %d.%d. Please check if cc-slurm-adapter is working correctly. If so, bump the version number in the source to suppress this warning.")
}

func callProcess(argv ...string) (string, error) {
	cmd := exec.Command(argv[0], argv[1:]...)

	var stdout bytes.Buffer
	cmd.Stdout = &stdout

	err := cmd.Run()
	if err != nil {
		return "", err
	}

	return stdout.String(), nil
}
