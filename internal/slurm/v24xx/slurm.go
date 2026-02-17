package slurm_v24xx

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"os/user"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ClusterCockpit/cc-slurm-adapter/internal/config"
	"github.com/ClusterCockpit/cc-slurm-adapter/internal/profiler"
	"github.com/ClusterCockpit/cc-slurm-adapter/internal/slurm/common"
	"github.com/ClusterCockpit/cc-slurm-adapter/internal/trace"
	"github.com/ClusterCockpit/cc-slurm-adapter/internal/types"

	"github.com/ClusterCockpit/cc-lib/v2/hostlist"
	"github.com/ClusterCockpit/cc-lib/v2/schema"
)

// SlurmInt supports these two JSON layouts:
// - 42
// - { "set": true, "infinite": false, "number": 42 }
type SlurmInt struct {
	Set      bool  `json:"set"`
	Infinite bool  `json:"infinite"`
	Number   int64 `json:"number"`
}

// SlurmString supports these two JSON layouts:
// - "myString"
// - [ "myString" ]
type SlurmString string

// SlurmIntString supports those two JSON layouts:
// - "123"
// - 123
type SlurmIntString string

type ScontrolJobResourcesNodesAllocationSocketCore struct {
	Index  *int         `json:"index"`
	Status *SlurmString `json:"status"`
}

type ScontrolJobResourcesNodesAllocationSocket struct {
	Index *int                                            `json:"index"`
	Cores []ScontrolJobResourcesNodesAllocationSocketCore `json:"cores"`
}

type ScontrolJobResourcesNodesAllocation struct {
	Hostname *string                                     `json:"name"`
	Sockets  []ScontrolJobResourcesNodesAllocationSocket `json:"sockets"`
	Index    *int                                        `json:"index"`
}

type ScontrolJobResourcesNodes struct {
	Allocation []ScontrolJobResourcesNodesAllocation `json:"allocation"`
}

type ScontrolJobResources struct {
	Nodes          *ScontrolJobResourcesNodes `json:"nodes"`
	ThreadsPerCore *SlurmInt                  `json:"threads_per_core"`
}

type ScontrolJob struct {
	// Only (our) required fields are listed here.
	JobId        *int64                `json:"job_id"`
	JobResources *ScontrolJobResources `json:"job_resources"`
	JobState     *SlurmString          `json:"job_state"`
	Comment      *string               `json:"comment"`
	Cluster      *string               `json:"cluster"`
	Partition    *string               `json:"partition"`
	Name         *string               `json:"name"`
	UserName     *string               `json:"user_name"`
	GroupName    *string               `json:"group_name"`
	Account      *string               `json:"account"`
	GresDetail   []string              `json:"gres_detail"`
	Shared       *SlurmString          `json:"shared"`
	Exclusive    *SlurmString          `json:"exclusive"`
	SubmitTime   *SlurmInt             `json:"submit_time"`
	StartTime    *SlurmInt             `json:"start_time"`
	EndTime      *SlurmInt             `json:"end_time"`
	TimeLimit    *SlurmInt             `json:"time_limit"`
	ArrayJobId   *SlurmInt             `json:"array_job_id"`
	TresReqStr   *string               `json:"tres_req_str"`
	TresAllocStr *string               `json:"tres_alloc_str"`
	CPUs         *SlurmInt             `json:"cpus"`
}

type ScontrolResult struct {
	Jobs []ScontrolJob `json:"jobs"`
	Meta *SlurmMeta    `json:"meta"`
}

type SacctJobState struct {
	Current *SlurmString `json:"current"`
}

type SacctJobArray struct {
	// Only (our) required fields are listed here.
	JobId *int64 `json:"job_id"`
}

type SlurmTresCount uint64

func (v *SlurmTresCount) UnmarshalJSON(data []byte) error {
	var num int64
	if err := json.Unmarshal(data, &num); err == nil {
		if num < 0 {
			*v = 0
		} else {
			*v = SlurmTresCount(num)
		}
		return nil
	}

	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		parsed, err := strconv.ParseInt(str, 10, 64)
		if err == nil {
			if parsed < 0 {
				*v = 0
			} else {
				*v = SlurmTresCount(parsed)
			}
			return nil
		}
		trace.Debug("Failed to parse SlurmTresCount: input='%s', error='%v'", string(data), err)
		return fmt.Errorf("Unable to parse '%s' as Slurm tres count: invalid format or possible Slurm version incompatibility", string(data))
	}

	trace.Debug("Failed to parse SlurmTresCount: input='%s', error='neither integer nor string'", string(data))
	return fmt.Errorf("Unable to parse '%s' as Slurm tres count: invalid format or possible Slurm version incompatibility", string(data))
}

type SacctJobTres struct {
	Type  *string         `json:"type"`
	Name  *string         `json:"name"`
	Id    *int32          `json:"id"`
	Count *SlurmTresCount `json:"count"`
}

type SacctJobTresList struct {
	Allocated []SacctJobTres `json:"allocated"`
	Requested []SacctJobTres `json:"requested"`
}

type SacctJob struct {
	// Only (our) required fields are listed here.
	Account         *string           `json:"account"`
	AllocationNodes *SlurmInt         `json:"allocation_nodes"`
	Array           *SacctJobArray    `json:"array"`
	Cluster         *string           `json:"cluster"`
	JobId           *int64            `json:"job_id"`
	Name            *string           `json:"name"`
	Partition       *string           `json:"partition"`
	Required        *SacctJobRequired `json:"required"`
	State           *SacctJobState    `json:"state"`
	Time            *SacctJobTime     `json:"time"`
	Script          *string           `json:"script"`
	User            *string           `json:"user"`
	Group           *string           `json:"group"`
	Nodes           *string           `json:"nodes"`
	Tres            *SacctJobTresList `json:"tres"`
}

type SacctJobRequired struct {
	CPUs          *SlurmInt `json:"CPUs"`
	MemoryPerCPU  *SlurmInt `json:"memory_per_cpu"`
	MemoryPerNode *SlurmInt `json:"memory_per_node"`
}

type SacctJobTime struct {
	// Only (our) required fields are listed here.
	Elapsed    SlurmInt `json:"elapsed"`
	End        SlurmInt `json:"end"`
	Limit      SlurmInt `json:"limit"`
	Start      SlurmInt `json:"start"`
	Submission SlurmInt `json:"submission"`
}

type SlurmMetaSlurmVersion struct {
	Major SlurmIntString `json:"major"`
	Minor SlurmIntString `json:"minor"`
	Micro SlurmIntString `json:"micro"`
}

type SlurmMetaSlurm struct {
	Version SlurmMetaSlurmVersion `json:"version"`
	Release string                `json:"release"`
	Cluster string                `json:"cluster"`
}

type SlurmMeta struct {
	// Only (our) required fields are listed here.
	Slurm SlurmMetaSlurm `json:"slurm"`
}

type SacctResult struct {
	// Only (our) required fields are listed here.
	Jobs []SacctJob `json:"jobs"`
	Meta SlurmMeta  `json:"meta"`
}

type SacctmgrUser struct {
	AdministratorLevel []string `json:"administrator_level"`
	Name               string   `json:"name"`
}

type SacctmgrCluster struct {
	Name *string `json:"name"`
}

type SacctmgrResult struct {
	Clusters []SacctmgrCluster `json:"clusters"`
	Users    []SacctmgrUser    `json:"users"`
	Meta     SlurmMeta         `json:"meta"`
}

type SinfoPartialNode struct {
	State []string `json:"state"`
}

type SinfoPartialNodes struct {
	Allocated *int     `json:"allocated"`
	Idle      *int     `json:"idle"`
	Total     *int     `json:"total"`
	Nodes     []string `json:"nodes"`
}

type SinfoPartialCpus struct {
	Allocated *int `json:"allocated"`
	Total     *int `json:"total"`
}

type SinfoPartialMemory struct {
	Maximum   *int   `json:"maximum"`
	Allocated *int64 `json:"allocated"`
	Free      *struct {
		Minimum SlurmInt `json:"minimum"`
		Maximum SlurmInt `json:"maximum"`
	} `json:"free"`
}

type SinfoPartialGres struct {
	Total *string `json:"total"`
	Used  *string `json:"used"`
}

type SinfoPartial struct {
	Node   *SinfoPartialNode   `json:"node"`
	Nodes  *SinfoPartialNodes  `json:"nodes"`
	Cpus   *SinfoPartialCpus   `json:"cpus"`
	Memory *SinfoPartialMemory `json:"memory"`
	Gres   *SinfoPartialGres   `json:"gres"`
}

type SinfoResult struct {
	Sinfo []SinfoPartial `json:"sinfo"`
	Meta  *SlurmMeta     `json:"meta"`
}

type GRES struct {
	Variant       string // e.g. "gpu"
	Id            string // e.g. "h100"
	Count         uint64 // e.g. "4"
	DomainType    string // e.g. "S", "IDX"
	DomainIndices []int
}

type Job struct {
	sa *SacctJob
	sc *ScontrolJob

	jobScript *string
	slurmInfo *string
}

const (
	SLURM_VERSION_INCOMPATIBLE string = "Unable to parse sacct JSON. Is cc-slurm-adapter compatible with this Slurm version?"
	SLURM_MAX_VER_MAJ          int    = 24
	SLURM_MAX_VER_MIN          int    = 11
)

type slurmApi struct {
	clusterNames []string
}

func NewSlurmApi() (slurm_common.SlurmApi, error) {
	api := &slurmApi{}

	// 1. Intial cluster query
	sacctmgrResult, err := QueryClusters()
	if err != nil {
		return nil, err
	}

	// 2. Is our version compatible?
	major, _ := strconv.Atoi(string(sacctmgrResult.Meta.Slurm.Version.Major))
	if major != 24 && major != 25 {
		return nil, fmt.Errorf("Slurm backend v24xx only supports Slurm version v24.XX and v25.XX (found '%s')", sacctmgrResult.Meta.Slurm.Version)
	}

	// 3. Determine cluster names managed by Slurm
	api.clusterNames = make([]string, 0)
	for _, clusterObj := range sacctmgrResult.Clusters {
		api.clusterNames = append(api.clusterNames, *clusterObj.Name)
	}
	trace.Debug("Detected Slurm clusters: %v", api.clusterNames)

	if len(api.clusterNames) == 0 {
		return nil, fmt.Errorf("Unable to determine cluster names. sacctmgr returned no clusters. Is this Slurm version compatible?")
	}

	// 4. Warn user if required Slurm permissions are not present
	CheckPerms()

	return api, nil
}

func (v *SlurmInt) UnmarshalJSON(data []byte) error {
	// Slurm at some point has changed the representation of integers in its API.
	// Unfortuantely the usage is somewhat mixed, so we use a custom integer type
	// with our own Unmarshal and Marshal functions. That way we can automatically
	// switch between the two variants and simply use "SlurmInt" as type in the structs
	// regardless of the Slurm version used.
	result := struct {
		Set      *bool  `json:"set"`
		Infinite *bool  `json:"infinite"`
		Number   *int64 `json:"number"`
	}{}
	err := json.Unmarshal(data, &result)
	if err == nil {
		if result.Set != nil && result.Infinite != nil && result.Number != nil {
			*v = SlurmInt{
				Set:      *result.Set,
				Infinite: *result.Infinite,
				Number:   *result.Number,
			}
			return nil
		}
	}

	result2, err := strconv.ParseInt(string(data), 10, 64)
	if err == nil {
		*v = SlurmInt{
			Set:      true,
			Infinite: false,
			Number:   result2,
		}
		return nil
	}

	return fmt.Errorf("Unable to parse '%s' as Slurm legacy integer nor new integer", string(data))
}

func (v *SlurmString) UnmarshalJSON(data []byte) error {
	// Slurm at some point wrapped strings in a list with just one string.
	// No idea why.
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

func (v *SlurmIntString) UnmarshalJSON(data []byte) error {
	// Slurm changed the usage of int to strings from v23 to v24 in its version field.
	// So allow this type to be parsed both ways.
	var resultStr string
	err := json.Unmarshal(data, &resultStr)
	if err == nil {
		*v = SlurmIntString(resultStr)
		return nil
	}

	var resultInt int
	err = json.Unmarshal(data, &resultInt)
	if err == nil {
		*v = SlurmIntString(fmt.Sprintf("%d", resultInt))
		return nil
	}
	return err
}

func QueryClusters() (*SacctmgrResult, error) {
	stdout, err := callProcess("sacctmgr", "list", "clusters", "--noheader", "--json")
	if err != nil {
		return nil, fmt.Errorf("Unable to run sacctmgr to obtain cluster names: %w", err)
	}

	result := &SacctmgrResult{}
	err = json.Unmarshal([]byte(stdout), result)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse sacctmgr output: %w", err)
	}

	return result, nil
}

func (api *slurmApi) GetClusterNames() []string {
	return api.clusterNames
}

func (api *slurmApi) QueryJobs(clusterName string, jobIds []int64) ([]slurm_common.Job, error) {
	retval := make([]slurm_common.Job, 0)

	if len(jobIds) == 0 {
		return retval, nil
	}

	jobIdStrings := make([]string, 0)

	jobQueries := make(map[int64]*Job)
	for _, jobId := range jobIds {
		jobQueries[jobId] = &Job{}
		jobIdStrings = append(jobIdStrings, fmt.Sprintf("%d", jobId))
	}

	jobIdString := strings.Join(jobIdStrings, ",")
	stdout, err := callProcess("sacct", "--cluster", clusterName, "-j", jobIdString, "--json")
	if err != nil {
		return nil, fmt.Errorf("Unable to run sacct -j %s: %w", jobIdString, err)
	}

	var result SacctResult
	err = json.Unmarshal([]byte(stdout), &result)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", SLURM_VERSION_INCOMPATIBLE, err)
	}

	// When a job ID is queried, which is part of an array job, all jobs related to this array job are returned.
	// Find the one that we actually want and filter out all the other ones
	for _, job := range result.Jobs {
		// Slurm may sometimes return more jobs than initially requested (e.g. for array jobs).
		// Ingore the jobs that were not requested.
		if _, ok := jobQueries[*job.JobId]; !ok {
			continue
		}

		jobQueries[*job.JobId] = &Job{sa: &job}
	}

	// Sanity check that we actually go what we requested.
	for jobId, job := range jobQueries {
		if job == nil {
			return nil, fmt.Errorf("Requested job (%s, %d) unavailable", clusterName, jobId)
		}
		retval = append(retval, job)
	}

	return retval, nil
}

func (api *slurmApi) QueryJobsTimeRange(clusterName string, begin, end time.Time) ([]slurm_common.Job, error) {
	starttime := begin.Format("2006-01-02T15:04:05") // e.g. '2025-02-24T15:00:00'
	endtime := end.Format("2006-01-02T15:04:05")     // e.g. '2025-02-24T15:00:00'
	stdout, err := callProcess("sacct", "--cluster", clusterName, "--allusers", "--starttime", starttime, "--endtime", endtime, "--json")
	if err != nil {
		return nil, fmt.Errorf("Unable to run sacct /w starttime/endtime: %w. (%s)", err, stdout)
	}

	var result SacctResult
	err = json.Unmarshal([]byte(stdout), &result)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", SLURM_VERSION_INCOMPATIBLE, err)
	}

	retval := make([]slurm_common.Job, len(result.Jobs))
	for i, job := range result.Jobs {
		retval[i] = &Job{sa: &job}
	}

	return retval, nil
}

func (api *slurmApi) QueryJobsActive(clusterName string) ([]slurm_common.Job, error) {
	// Caution: it is important to use --noheader here.
	// For multi cluster systems squeue will otherwise print non-JSON header lines.
	stdout, err := callProcess("squeue", "--noheader", "--cluster", clusterName, "--all", "--json")
	if err != nil {
		return nil, fmt.Errorf("Unable to run squeue: %w", err)
	}

	var result ScontrolResult // scontrol and squeue appear to use the same scheme
	err = json.Unmarshal([]byte(stdout), &result)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", SLURM_VERSION_INCOMPATIBLE, err)
	}

	scontrolJobsCommon := make([]slurm_common.Job, len(result.Jobs))
	for i, job := range result.Jobs {
		scontrolJobsCommon[i] = &Job{sc: &job}
	}

	return scontrolJobsCommon, nil
}

func (api *slurmApi) QueryJobsWithResources(clusterName string, jobs []slurm_common.Job) error {
	if len(jobs) == 0 {
		return nil
	}

	jobIdStrings := make([]string, 0)
	jobMap := make(map[int64]slurm_common.Job)

	for _, job := range jobs {
		if job.GetCluster() != clusterName {
			trace.Fatal("BUG: Cannot query job for mismatching cluster")
		}

		if !job.HasResourceInfo() {
			jobIdStrings = append(jobIdStrings, fmt.Sprintf("%d", job.GetJobId()))
			jobMap[job.GetJobId()] = job
		}
	}

	jobIdString := strings.Join(jobIdStrings, ",")
	stdout, err := callProcess("squeue", "--noheader", "--cluster", clusterName, "-j", jobIdString, "--json")
	if err != nil {
		return fmt.Errorf("Unable to run squeue -j %s: %w", jobIdString, err)
	}

	var result ScontrolResult
	err = json.Unmarshal([]byte(stdout), &result)
	if err != nil {
		return fmt.Errorf("%s: %w", SLURM_VERSION_INCOMPATIBLE, err)
	}

	for _, jobResult := range result.Jobs {
		if job, ok := jobMap[*jobResult.JobId]; ok {
			jobReal := job.(*Job)
			jobReal.sc = &jobResult
		}
	}

	return nil
}

func GetNodes(job *SacctJob) ([]string, error) {
	if strings.ToLower(*job.Nodes) == "none assigned" {
		// Jobs, which have been cancelled before being scheduled, won't have any
		// hostnames listed. Return an empty list in this case.
		return make([]string, 0), nil
	}

	nodeList, err := hostlist.Expand(*job.Nodes)
	if err != nil {
		return nil, fmt.Errorf("Unable to resolve hostname list '%s': %w", *job.Nodes, err)
	}

	return nodeList, nil
}

func CheckPerms() {
	trace.Debug("SlurmCheckPerms()")

	// This function checks, whether we are a Slurm operator. Issue a warning
	// if we are not.
	userObj, err := user.Current()
	if err != nil {
		trace.Fatal("Unable to retrieve current user name: %s", err)
	}
	username := userObj.Username

	errBase := "Unable to check whether we have appropriate Slurm permissions (%s). cc-slurm-adapter MAY NOT REPORY ANY JOBS!"

	stdout, err := callProcess("sacctmgr", "show", "user", username, "--json")
	if err != nil {
		trace.Warn(errBase, fmt.Sprintf("sacctmgr: %s", err))
		return
	}

	var result SacctmgrResult
	err = json.Unmarshal([]byte(stdout), &result)
	if err != nil {
		trace.Warn(errBase, fmt.Sprintf("JSON: %s", err))
		return
	}

	trace.Debug("Checking permissions for user: %s", username)
	trace.Debug("Users returned: %s", stdout)

	for _, curUser := range result.Users {
		if curUser.Name != username {
			continue
		}
		if slices.Contains(curUser.AdministratorLevel, "Operator") {
			trace.Debug("sacctmgr: Successfully detected Slurm Operator permissions!")
			return
		}
	}

	trace.Warn("sacctmgr reported that our user '%s' is not a Slurm operator. If Slurm uses relaxed permissions, this is not a problem. However, if not, NO JOBS WILL BE REPORTED! Run 'sacctmgr add user %s Account=root AdminLevel=operator'", username, username)
}

func (api *slurmApi) QueryClusterStats(cluster string) (*SinfoResult, error) {
	trace.Debug("QueryClusterStats()")

	stdout, err := callProcess("sinfo", "--cluster", cluster, "--noheader", "--json")
	if err != nil {
		return nil, fmt.Errorf("sinfo on cluster '%s' failed: %w", cluster, err)
	}

	var result SinfoResult
	err = json.Unmarshal([]byte(stdout), &result)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse sinfo JSON: %w", err)
	}

	return &result, nil
}

func (api *slurmApi) QueryNodeStats(cluster string) ([]types.CCNodeStat, error) {
	// Obtain various cluster stats like used CPUs, GPUs, etc.
	stats, err := api.QueryClusterStats(cluster)
	if err != nil {
		trace.Error("Unable to sync Slurm stats to cc-backend: %v", err)
		return nil, nil
	}

	ccNodeStats := make([]types.CCNodeStat, 0)

	nodesMap := make(map[string]types.CCNodeStat)

	for _, stat := range stats.Sinfo {
		for _, hostname := range stat.Nodes.Nodes {
			node, ok := nodesMap[hostname]
			if !ok {
				node = types.CCNodeStat{}
			}

			node.Hostname = hostname
			// For some reason the CPU core counts are aggregated over the number of nodes
			node.CpusAllocated = *stat.Cpus.Allocated / len(stat.Nodes.Nodes)
			//node.CpusTotal = *stat.Cpus.Total / len(stat.Nodes.Nodes)
			// Memory is not aggragated
			node.MemoryAllocated = *stat.Memory.Allocated
			//node.MemoryTotal = *stat.Memory.Maximum
			// Neither is GRES
			gresAlloc, errAlloc := ParseGRES(*stat.Gres.Used)
			_, errTotal := ParseGRES(*stat.Gres.Total)
			if errTotal == nil && errAlloc == nil {
				//node.GpusTotal = int(gresTotal.Count)
				node.GpusAllocated = int(gresAlloc.Count)
			} else {
				//node.GpusTotal = 0
				node.GpusAllocated = 0
			}

			for _, state := range stat.Node.State {
				if !slices.Contains(node.States, state) {
					node.States = append(node.States, state)
				}
			}

			nodesMap[hostname] = node
		}
	}

	for _, node := range nodesMap {
		ccNodeStats = append(ccNodeStats, node)
	}

	return ccNodeStats, nil
}

func ParseGRES(gres string) (*GRES, error) {
	// e.g. "gpu:h100:4(IDX:0-3)" --> "gpu" "h100" "4" "IDX" "0-3"
	gresParseRegex := regexp.MustCompile("^(\\w+):(\\w+):(\\d+)\\((\\w+):([0-9,\\-]+)\\)$")
	gresParsed := gresParseRegex.FindStringSubmatch(gres)
	if len(gresParsed) != 6 {
		return nil, fmt.Errorf("Unable to parse GRES: '%s'", gres)
	}

	count, err := strconv.ParseUint(gresParsed[3], 10, 64)
	if err != nil {
		return nil, err
	}

	return &GRES{
		Variant:       gresParsed[1],
		Id:            gresParsed[2],
		Count:         count,
		DomainType:    gresParsed[4],
		DomainIndices: rangeStringToInts(gresParsed[5]),
	}, nil
}

func rangeStringToInts(rangeString string) []int {
	// commaList: ["0-2", "5"]
	result := make([]int, 0)
	commaList := strings.Split(rangeString, ",")
	for _, subRange := range commaList {
		subRangeElements := strings.Split(subRange, "-")
		if len(subRangeElements) == 1 {
			i, err := strconv.Atoi(subRangeElements[0])
			if err != nil {
				continue
			}
			result = append(result, i)
			continue
		}

		if len(subRangeElements) != 2 {
			continue
		}

		first, err := strconv.Atoi(subRangeElements[0])
		if err != nil {
			continue
		}

		last, err := strconv.Atoi(subRangeElements[1])
		if err != nil {
			continue
		}

		for i := first; i <= last; i++ {
			result = append(result, i)
		}
	}

	return result
}

func callProcess(argv ...string) (string, error) {
	profiler.SectionBegin(fmt.Sprintf("PROCESS %s", argv[0]))
	defer profiler.SectionEnd(fmt.Sprintf("PROCESS %s", argv[0]))

	trace.Debug("Running command: %#v", argv)
	cmd := exec.Command(argv[0], argv[1:]...)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		return fmt.Sprintf("stdout: %s, stdout: %s", stdout.String(), stderr.String()), err
	}

	return stdout.String(), nil
}

func saScNilErr() {
	trace.Fatal("Trying to read from Job with neither sacct or scontrol info (sa=nil & sc=nil)")
}

func (j *Job) GetJobId() int64 {
	if j.sa != nil {
		return *j.sa.JobId
	}
	if j.sc != nil {
		return *j.sc.JobId
	}
	saScNilErr()
	return -1
}

func (j *Job) GetCluster() string {
	if j.sa != nil {
		return *j.sa.Cluster
	}
	if j.sc != nil {
		return *j.sc.Cluster
	}
	saScNilErr()
	return ""
}

func (j *Job) GetPartition() string {
	if j.sa != nil {
		return *j.sa.Partition
	}
	if j.sc != nil {
		return *j.sc.Partition
	}
	saScNilErr()
	return ""
}

func (j *Job) GetName() string {
	if j.sa != nil {
		return *j.sa.Name
	}
	if j.sc != nil {
		return *j.sc.Name
	}
	saScNilErr()
	return ""
}

func (j *Job) GetUser() string {
	if j.sa != nil {
		return *j.sa.User
	}
	if j.sc != nil {
		return *j.sc.UserName
	}
	saScNilErr()
	return ""
}

func (j *Job) GetGroup() string {
	if j.sa != nil {
		return *j.sa.Group
	}
	if j.sc != nil {
		return *j.sc.GroupName
	}
	saScNilErr()
	return ""
}

func (j *Job) GetAccount() string {
	if j.sa != nil {
		return *j.sa.Account
	}
	if j.sc != nil {
		return *j.sc.Account
	}
	saScNilErr()
	return ""
}

func (j *Job) GetState() string {
	if j.sa != nil {
		return string(*j.sa.State.Current)
	}
	if j.sc != nil {
		return string(*j.sc.JobState)
	}
	saScNilErr()
	return ""
}

func (j *Job) IsFinished() bool {
	if j.sa != nil {
		return j.sa.Time.End.Number > 0
	}
	if j.sc != nil {
		return j.sc.EndTime.Number > 0
	}
	saScNilErr()
	return false
}

func (j *Job) GetSubmitTime() time.Time {
	if j.sa != nil {
		return time.Unix(j.sa.Time.Submission.Number, 0)
	}
	if j.sc != nil {
		return time.Unix(j.sc.SubmitTime.Number, 0)
	}
	saScNilErr()
	return time.Unix(0, 0)
}

func (j *Job) GetStartTime() time.Time {
	if j.sa != nil {
		return time.Unix(j.sa.Time.Start.Number, 0)
	}
	if j.sc != nil {
		return time.Unix(j.sc.StartTime.Number, 0)
	}
	saScNilErr()
	return time.Unix(0, 0)
}

func (j *Job) GetEndTime() time.Time {
	if j.sa != nil {
		return time.Unix(j.sa.Time.End.Number, 0)
	}
	if j.sc != nil {
		return time.Unix(j.sc.EndTime.Number, 0)
	}
	saScNilErr()
	return time.Unix(0, 0)
}

func (j *Job) GetTimeLimit() time.Duration {
	if j.sa != nil {
		return time.Duration(j.sa.Time.Limit.Number) * time.Minute // slurm reports the limit in MINUTES, not seconds
	}
	if j.sc != nil {
		return time.Duration(j.sc.TimeLimit.Number) * time.Minute
	}
	saScNilErr()
	return time.Duration(0)
}

func (j *Job) GetResources() ([]*schema.Resource, error) {
	// TODO rewrite this function for the new interface
	// This function fetches additional information about a Slurm job via scontrol.
	// Unfortunately some of the information is not available via sacct, so we need
	// scontrol to get this information. Because this information is not stored
	// in the slurmdbd, we have to query this within a few minutes after a job has
	// terminated at last.
	// If this fetching fails, we cannot populate allocated resources. This is not
	// critical to the operation of cc-backend, but it means certains graphs won't be
	// available, since metrics won't be assignable to a job anymore.

	// Create schema.Resources out of the ScontrolResult
	if j.sc == nil {
		// If no jobs are returned, this is most likely because the job has already ended some time ago.
		// There is nothing we can do about this, so try to obtain hostnames
		// and continue without hwthread information.
		// You can reduce the chances of this by increasing MinJobAge in slurm.conf
		nodes, err := GetNodes(j.sa)
		if err != nil {
			return nil, fmt.Errorf("scontrol returned no jobs for id %d and we were unable to obtain node names: %w", *j.sa.JobId, err)
		}
		trace.Debug("Job (%s, %d) has already ended. Hostnames are the only available resource.", *j.sa.Cluster, *j.sa.JobId)
		resources := make([]*schema.Resource, len(nodes))
		for i, v := range nodes {
			resources[i] = &schema.Resource{Hostname: v}
		}
		return resources, nil
	}

	if j.sc.JobResources == nil || j.sc.JobResources.Nodes == nil {
		// If Resources is nil, then the job probably just hasn't started yet.
		// we can safely return an empty list, since this job will be discarded
		// later either way.
		trace.Debug("Job (%s, %d) has scontrol info available, but no resources", *j.sa.Cluster, *j.sa.JobId)
		return make([]*schema.Resource, 0), nil
	}

	scAllocation := j.sc.JobResources.Nodes.Allocation
	resources := make([]*schema.Resource, 0)
	for _, allocation := range scAllocation {
		// Determine Hwthreads
		hwthreads := make([]int, 0)
		cpusPerSocket := len(allocation.Sockets[0].Cores)
		for _, socket := range allocation.Sockets {
			for _, core := range socket.Cores {
				if string(*core.Status) != "ALLOCATED" {
					continue
				}
				hwthreads = append(hwthreads, *socket.Index*cpusPerSocket+*core.Index)
			}
		}

		// Determine accelerators. We prefer to get the information via Config + GresDetail.
		// Though, for legacy we also support parsing the comment field.
		// The latter one requires manual intervention by the Slurm Administrators.
		var accelerators []string
		if *allocation.Index < len(j.sc.GresDetail) {
			trace.Debug("Detecting GPU via gres")
			nodeGres, err := ParseGRES(j.sc.GresDetail[*allocation.Index])
			if err == nil && nodeGres.Variant == "gpu" {
				found := false
				for hostRegex, pciAddrList := range config.Config.GpuPciAddrs {
					// We initially check the regex, so no need to check for errors again.
					match, _ := regexp.MatchString(hostRegex, *allocation.Hostname)
					if match {
						for _, v := range nodeGres.DomainIndices {
							if v >= len(pciAddrList) {
								trace.Error("Unable to determine PCI address: Detected GPU in job %d, which is not listed in config file (gresIndex=%d >= len(gpus)=%d)", *j.sa.JobId, v, len(config.Config.GpuPciAddrs))
								continue
							}
							trace.Debug("Found GPU %d for %s: %s", v, *allocation.Hostname, pciAddrList[v])
							accelerators = append(accelerators, pciAddrList[v])
						}
						found = true
						break
					}
				}
				if !found {
					trace.Warn("Unable to find GPU list for hostname=%s from GRES for job %d", *allocation.Hostname, *j.sa.JobId)
				}
			}
		} else if *j.sc.Comment != "" {
			trace.Debug("Detecting GPU via comment")
			accelerators = strings.Split(*j.sc.Comment, ",")
		}

		// Create final result
		r := schema.Resource{
			Hostname:     *allocation.Hostname,
			HWThreads:    hwthreads,
			Accelerators: accelerators,
		}
		resources = append(resources, &r)
	}

	return resources, nil
}

func (j *Job) GetJobScript() string {
	if j.jobScript == nil {
		stdout, err := callProcess("scontrol", "--cluster", j.GetCluster(), "write", "batch_script", fmt.Sprintf("%d", j.GetJobId()), "-")
		if err != nil {
			// If the job has ended some time ago, this will fail.
			// However, this is not a critical case, so just return an empty job script.
			stdout = ""
		}
		j.jobScript = &stdout
	}

	return *j.jobScript
}

func (j *Job) GetSlurmInfo() string {
	if j.slurmInfo == nil {
		stdout, err := callProcess("scontrol", "--cluster", j.GetCluster(), "show", "job", fmt.Sprintf("%d", j.GetJobId()))
		if err != nil {
			// If query fails, this is most likely because the job has already ended some time ago.
			// There is nothing we can do about this, so continue with just a warning.
			return fmt.Sprintf("Error while getting job information for (%s, %d)", j.GetCluster(), j.GetJobId())
		}

		arrayJobGapIndex := strings.Index(stdout, "\n\n")
		if arrayJobGapIndex != -1 {
			stdout = stdout[0 : arrayJobGapIndex+1]
		}

		tmp := strings.TrimSpace(stdout)
		j.slurmInfo = &tmp
	}

	return *j.slurmInfo
}

func (j *Job) GetArrayJobId() int64 {
	if j.sa != nil {
		return *j.sa.Array.JobId
	}
	if j.sc != nil {
		return j.sc.ArrayJobId.Number
	}
	saScNilErr()
	return -1
}

func (j *Job) GetNumNodes() int32 {
	if j.sa != nil {
		return int32(j.sa.AllocationNodes.Number)
	}
	if j.sc != nil {
		return int32(len(j.sc.JobResources.Nodes.Allocation))
	}
	saScNilErr()
	return -1
}

func GetSacctTresCount(tresList []SacctJobTres, tresType, tresName string, count *int32) {
	for _, tres := range tresList {
		if *tres.Type == tresType {
			if tresName != "" && *tres.Name != tresName {
				continue
			}
			*count = int32(*tres.Count)
			return
		}
	}
	// If no valid tres is found, "count" remains unchanged
}

func GetScontrolTresCount(tresStr string, tresType, tresName string, count *int32) {
	// tresStr looks like this: "cpu=1,mem=1M,node=1,billing=1"
	tresCheck := ""
	if tresName != "" {
		tresCheck = fmt.Sprintf("%s/%s", tresType, tresName)
	} else {
		tresCheck = tresType
	}

	tresList := strings.Split(tresStr, ",")
	for _, tres := range tresList {
		// tres looks like this: "cpu=1"

		tresSplit := strings.Split(tres, "=")
		if len(tresSplit) != 2 {
			trace.Error("tres element does not consist of two parts: '%s' (from '%s')", tresStr, tres)
			continue
		}

		if tresSplit[0] == tresCheck {
			v, err := strconv.ParseInt(tresSplit[1], 10, 64)
			if err != nil {
				trace.Error("Unable to parse tres count: %v", err)
				continue
			}

			*count = int32(v)
			return
		}
	}
}

func (j *Job) GetNumHWThreads() int32 {
	if j.sa != nil {
		result := int32(j.sa.Required.CPUs.Number)
		GetSacctTresCount(j.sa.Tres.Requested, "cpu", "", &result)
		GetSacctTresCount(j.sa.Tres.Allocated, "cpu", "", &result)
		return result
	}
	if j.sc != nil {
		result := int32(j.sc.CPUs.Number)
		GetScontrolTresCount(*j.sc.TresReqStr, "cpu", "", &result)
		GetScontrolTresCount(*j.sc.TresAllocStr, "cpu", "", &result)
		return result
	}
	saScNilErr()
	return -1
}

func (j *Job) GetNumAccelerators() int32 {
	if j.sa != nil {
		result := int32(0)
		GetSacctTresCount(j.sa.Tres.Requested, "gres", "gpu", &result)
		GetSacctTresCount(j.sa.Tres.Allocated, "gres", "gpu", &result)
		return result
	}
	if j.sc != nil {
		result := int32(0)
		GetScontrolTresCount(*j.sc.TresReqStr, "gres", "gpu", &result)
		GetScontrolTresCount(*j.sc.TresAllocStr, "gres", "gpu", &result)
		return result
	}
	saScNilErr()
	return -1
}

func (j *Job) GetNodeShared() string {
	shared := "multi_user"
	if j.sc != nil {
		if j.sc.Exclusive != nil && string(*j.sc.Exclusive) == "true" {
			shared = "none"
		} else if j.sc.Shared != nil {
			if string(*j.sc.Shared) == "user" {
				shared = "single_user"
			} else if string(*j.sc.Shared) == "none" {
				shared = "none"
			} else if string(*j.sc.Shared) == "" {
				shared = "multi_user"
			}
		} else {
			trace.Debug("No information available about exclusive/shared for job %d.", *j.sc.JobId)
		}
	}

	// Do not require sacct information here, so don't throw an error if it's nil.
	// Afaik this is not stored in the Slurm database, so we can't really do anthing
	// but return a default value. Perhaps in the future it would make more sense to
	// return something like "unknown".

	return shared
}

func (j *Job) HasResourceInfo() bool {
	return j.sc != nil
}

func (j *Job) HasDbInfo() bool {
	return j.sa != nil
}
