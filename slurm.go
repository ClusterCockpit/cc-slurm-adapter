package main

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

	"github.com/ClusterCockpit/cc-backend/pkg/schema"
	"github.com/ClusterCockpit/cc-slurm-adapter/trace"
)

/* SlurmInt supports these two JSON layouts:
 * - 42
 * - { "set": true, "infinite": false, "number": 42 }
 */
type SlurmInt struct {
	Set      bool  `json:"set"`
	Infinite bool  `json:"infinite"`
	Number   int64 `json:"number"`
}

/* SlurmString supports these two JSON layouts:
 * - "myString"
 * - [ "myString" ]
 */
type SlurmString string

/* SlurmIntString supports those two JSON layouts:
 * - "123"
 * - 123
 */
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
	CPUs           *SlurmInt                  `json:"cpus"`
	ThreadsPerCore *SlurmInt                  `json:"threads_per_core"`
}

type ScontrolJob struct {
	/* Only (our) required fields are listed here. */
	JobId        *uint32               `json:"job_id"`
	JobResources *ScontrolJobResources `json:"job_resources"`
	JobState     *SlurmString          `json:"job_state"`
	Comment      *string               `json:"comment"`
	Cluster      *string               `json:"cluster"`
	GresDetail   []string              `json:"gres_detail"`
	Shared       *SlurmString          `json:"shared"`
	Exclusive    *SlurmString          `json:"exclusive"`
}

type ScontrolResult struct {
	Jobs []ScontrolJob `json:"jobs"`
	Meta *SlurmMeta    `json:"meta"`
}

type SacctJobState struct {
	Current *SlurmString `json:"current"`
}

type SacctJobArray struct {
	/* Only (our) required fields are listed here. */
	JobId *uint32 `json:"job_id"`
}

type SacctJobTres struct {
	Type  *string `json:"type"`
	Name  *string `json:"name"`
	Id    *int32  `json:"id"`
	Count *int32  `json:"count"`
}

type SacctJobTresList struct {
	Allocated []SacctJobTres `json:"allocated"`
	Requested []SacctJobTres `json:"requested"`
}

type SacctJob struct {
	/* Only (our) required fields are listed here. */
	Account         *string           `json:"account"`
	AllocationNodes *SlurmInt         `json:"allocation_nodes"`
	Array           *SacctJobArray    `json:"array"`
	Cluster         *string           `json:"cluster"`
	JobId           *uint32           `json:"job_id"`
	Name            *string           `json:"name"`
	Partition       *string           `json:"partition"`
	Required        *SacctJobRequired `json:"required"`
	State           *SacctJobState    `json:"state"`
	Time            *SacctJobTime     `json:"time"`
	Script          *string           `json:"script"`
	User            *string           `json:"user"`
	Nodes           *string           `json:"nodes"`
	Tres            *SacctJobTresList `json:"tres"`
}

type SacctJobRequired struct {
	CPUs          *SlurmInt `json:"CPUs"`
	MemoryPerCPU  *SlurmInt `json:"memory_per_cpu"`
	MemoryPerNode *SlurmInt `json:"memory_per_node"`
}

type SacctJobTime struct {
	/* Only (our) required fields are listed here. */
	Elapsed SlurmInt `json:"elapsed"`
	End     SlurmInt `json:"end"`
	Limit   SlurmInt `json:"limit"`
	Start   SlurmInt `json:"start"`
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
	/* Only (our) required fields are listed here. */
	Slurm SlurmMetaSlurm `json:"slurm"`
}

type SacctResult struct {
	/* Only (our) required fields are listed here. */
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
	Maximum   *int `json:"maximum"`
	Allocated *int `json:"allocated"`
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
	Count         int    // e.g. "4"
	DomainType    string // e.g. "S", "IDX"
	DomainIndices []int
}

const (
	SLURM_VERSION_INCOMPATIBLE string = "Unable to parse sacct JSON. Is cc-slurm-adapter compatible with this Slurm version?"
	SLURM_MAX_VER_MAJ          int    = 24
	SLURM_MAX_VER_MIN          int    = 11
)

var (
	// We keep a cached version of the Sacct results, since we may otherwise need to execute the same sacct command multiple times per batch run
	sacctCache map[string]map[uint32]*SacctJob
)

func (v *SlurmInt) UnmarshalJSON(data []byte) error {
	/* Slurm at some point has changed the representation of integers in its API.
	 * Unfortuantely the usage is somewhat mixed, so we use a custom integer type
	 * with our own Unmarshal and Marshal functions. That way we can automatically
	 * switch between the two variants and simply use "SlurmInt" as type in the structs
	 * regardless of the Slurm version used. */
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

func (v *SlurmIntString) UnmarshalJSON(data []byte) error {
	/* Slurm changed the usage of int to strings from v23 to v24 in its version field.
	 * So allow this type to be parsed both ways. */
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

func SlurmGetClusterNames() ([]string, error) {
	stdout, err := callProcess("sacctmgr", "list", "clusters", "--noheader", "--json")
	if err != nil {
		return nil, fmt.Errorf("Unable to run sacctmgr to obtain cluster names: %w", err)
	}

	var result SacctmgrResult
	err = json.Unmarshal([]byte(stdout), &result)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse sacctmgr output: %w", err)
	}

	clusterNames := make([]string, 0)
	for _, clusterObj := range result.Clusters {
		clusterNames = append(clusterNames, *clusterObj.Name)
	}

	if len(clusterNames) == 0 {
		return nil, fmt.Errorf("Unable to determine cluster names. sacctmgr returned no clusters. Is this Slurm version compatible?")
	}

	return clusterNames, nil
}

func SlurmSacctCacheClear() {
	trace.Debug("Clearing Slurm sacct cache")
	sacctCache = make(map[string]map[uint32]*SacctJob)
}

func slurmSacctCacheAdd(job *SacctJob) {
	if sacctCache == nil {
		sacctCache = make(map[string]map[uint32]*SacctJob)
	}
	if sacctCache[*job.Cluster] == nil {
		sacctCache[*job.Cluster] = make(map[uint32]*SacctJob)
	}
	sacctCache[*job.Cluster][*job.JobId] = job
}

func SlurmQueryJob(clusterName string, jobId uint32) (*SacctJob, error) {
	if sacctCache[clusterName] != nil && sacctCache[clusterName][jobId] != nil {
		trace.Debug("Job (%s, %d) already in cache, skipping 'sacct'", clusterName, jobId)
		return sacctCache[clusterName][jobId], nil
	}

	// Performance info: This can be fairly expensive, hence why we have some sort of caching.
	// You may be able to do ~5 sacct calls per second.
	stdout, err := callProcess("sacct", "--cluster", clusterName, "-j", fmt.Sprintf("%d", jobId), "--json")
	if err != nil {
		return nil, fmt.Errorf("Unable to run sacct -j %d: %w", jobId, err)
	}

	var result SacctResult
	err = json.Unmarshal([]byte(stdout), &result)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", SLURM_VERSION_INCOMPATIBLE, err)
	}

	SlurmWarnVersion(result.Meta.Slurm.Version)

	// When a job ID is queried, which is part of an array job, all jobs related to this array job are returned.
	// Find the one that we actually want.
	for _, job := range result.Jobs {
		if *job.JobId == jobId {
			slurmSacctCacheAdd(&job)
			return &job, nil
		}
	}
	if len(result.Jobs) == 0 {
		return nil, fmt.Errorf("Requested job (%s, %d) unavailable", clusterName, jobId)
	}
	return nil, fmt.Errorf("Requested job (%s, %d) returned jobs, but none with our job ID: %+v", clusterName, jobId, result.Jobs)
}

func SlurmQueryJobsTimeRange(clusterName string, begin, end time.Time) ([]SacctJob, error) {
	starttime := begin.Format(time.DateTime) // e.g. '2025-02-24 15:00'
	endtime := end.Format(time.DateTime)     // e.g. '2025-02-24 15:00'
	stdout, err := callProcess("sacct", "--cluster", clusterName, "--allusers", "--starttime", starttime, "--endtime", endtime, "--json")
	if err != nil {
		return nil, fmt.Errorf("Unable to run sacct /w starttime/endtime: %w. (%s)", err, stdout)
	}

	var result SacctResult
	err = json.Unmarshal([]byte(stdout), &result)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", SLURM_VERSION_INCOMPATIBLE, err)
	}

	SlurmWarnVersion(result.Meta.Slurm.Version)

	for _, job := range result.Jobs {
		slurmSacctCacheAdd(&job)
	}

	return result.Jobs, nil
}

func SlurmQueryJobsActive(clusterName string) ([]ScontrolJob, error) {
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

	SlurmWarnVersion(result.Meta.Slurm.Version)
	return result.Jobs, nil
}

func SlurmGetScontrolJob(job SacctJob) (*ScontrolJob, error) {
	// Performance info: This scontrol is usually fairly quickly, since this doesn't query the slurmdbd.
	// In my tests it was around 100 executions per second.
	stdout, err := callProcess("scontrol", "--cluster", *job.Cluster, "show", "job", fmt.Sprintf("%d", *job.JobId), "--json")
	if err != nil {
		return nil, fmt.Errorf("Unable to run scontrol show job %d: %w (%s)", *job.JobId, err, stdout)
	}

	var scResult ScontrolResult
	err = json.Unmarshal([]byte(stdout), &scResult)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse scontrol JSON: %w", err)
	}

	if len(scResult.Jobs) == 0 {
		return nil, nil
	}

	if len(scResult.Jobs) > 1 {
		for _, scJob := range scResult.Jobs {
			if *scJob.JobId == *job.JobId {
				return &scJob, nil
			}
		}
		return nil, fmt.Errorf("'scontrol show job %d' returned too many jobs (%d > 1). The one we were looking for was not part of it.", *job.JobId, len(scResult.Jobs))
	}

	return &scResult.Jobs[0], nil
}

func SlurmGetResources(saJob SacctJob, scJob *ScontrolJob) ([]*schema.Resource, error) {
	/* This function fetches additional information about a Slurm job via scontrol.
	 * Unfortunately some of the information is not available via sacct, so we need
	 * scontrol to get this information. Because this information is not stored
	 * in the slurmdbd, we have to query this within a few minutes after a job has
	 * terminated at last.
	 * If this fetching fails, we cannot populate allocated resources. This is not
	 * critical to the operation of cc-backend, but it means certains graphs won't be
	 * available, since metrics won't be assignable to a job anymore. */

	/* Create schema.Resources out of the ScontrolResult */
	if scJob == nil {
		/* If no jobs are returned, this is most likely because the job has already ended some time ago.
		 * There is nothing we can do about this, so try to obtain hostnames
		 * and continue without hwthread information. */
		nodes, err := SlurmGetNodes(saJob)
		if err != nil {
			return nil, fmt.Errorf("scontrol returned no jobs for id %d and we were unable to obtain node names: %w", *saJob.JobId, err)
		}
		trace.Debug("Job (%s, %d) has already ended. Hostnames are the only available resource.", *saJob.Cluster, *saJob.JobId)
		resources := make([]*schema.Resource, len(nodes))
		for i, v := range nodes {
			resources[i] = &schema.Resource{Hostname: v}
		}
		return resources, nil
	}

	if scJob.JobResources == nil || scJob.JobResources.Nodes == nil {
		/* If Resources is nil, then the job probably just hasn't started yet.
		 * we can safely return an empty list, since this job will be discarded
		 * later either way. */
		trace.Debug("Job (%s, %d) has scontrol info available, but no resources", *saJob.Cluster, *saJob.JobId)
		return make([]*schema.Resource, 0), nil
	}

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
				hwthreads = append(hwthreads, *socket.Index*cpusPerSocket+*core.Index)
			}
		}

		/* Determine accelerators. We prefer to get the information via Config + GresDetail.
		 * Though, for legacy we also support parsing the comment field.
		 * The latter one requires manual intervention by the Slurm Administrators. */
		var accelerators []string
		if *allocation.Index < len(scJob.GresDetail) {
			trace.Debug("Detecting GPU via gres")
			nodeGres, err := SlurmParseGRES(scJob.GresDetail[*allocation.Index])
			if err == nil && nodeGres.Variant == "gpu" {
				found := false
				for hostRegex, pciAddrList := range Config.GpuPciAddrs {
					/* We initially check the regex, so no need to check for errors again. */
					match, _ := regexp.MatchString(hostRegex, *allocation.Hostname)
					if match {
						for _, v := range nodeGres.DomainIndices {
							if v >= len(pciAddrList) {
								trace.Error("Unable to determine PCI address: Detected GPU in job %d, which is not listed in config file (gresIndex=%d >= len(gpus)=%d)", *saJob.JobId, v, len(Config.GpuPciAddrs))
								continue
							}
							trace.Debug("Found GPU %d for %s: %s", v, *allocation.Hostname, pciAddrList[v])
							accelerators = append(accelerators, pciAddrList[v])
						}
						found = true
					}
				}
				if !found {
					trace.Warn("Unable to find GPU list for hostname=%s from GRES for job %d", *allocation.Hostname, *saJob.JobId)
				}
			}
		} else if *scJob.Comment != "" {
			trace.Debug("Detecting GPU via comment")
			accelerators = strings.Split(*scJob.Comment, ",")
		}

		/* Create final result */
		r := schema.Resource{
			Hostname:     *allocation.Hostname,
			HWThreads:    hwthreads,
			Accelerators: accelerators,
		}
		resources = append(resources, &r)
	}

	return resources, nil
}

func SlurmGetNodes(job SacctJob) ([]string, error) {
	if strings.ToLower(*job.Nodes) == "none assigned" {
		/* Jobs, which have been cancelled before being scheduled, won't have any
		 * hostnames listed. Return an empty list in this case. */
		return make([]string, 0), nil
	}
	stdout, err := callProcess("scontrol", "--cluster", *job.Cluster, "show", "hostnames", *job.Nodes)
	if err != nil {
		return nil, fmt.Errorf("scontrol show hostnames '%s' failed: %w (%s)", *job.Nodes, err, stdout)
	}
	stdout = strings.TrimSpace(stdout)
	return strings.Split(stdout, "\n"), nil
}

func SlurmGetJobInfoText(job SacctJob) string {
	stdout, err := callProcess("scontrol", "--cluster", *job.Cluster, "show", "job", fmt.Sprintf("%d", *job.JobId))
	if err != nil {
		/* If query fails, this is most likely because the job has already ended some time ago.
		 * There is nothing we can do about this, so continue with just a warning. */
		return fmt.Sprintf("Error while getting job information for JobID=%d", *job.JobId)
	}

	arrayJobGapIndex := strings.Index(stdout, "\n\n")
	if arrayJobGapIndex != -1 {
		stdout = stdout[0 : arrayJobGapIndex+1]
	}

	return strings.TrimSpace(stdout)
}

func SlurmGetJobScript(job SacctJob) string {
	stdout, err := callProcess("scontrol", "--cluster", *job.Cluster, "write", "batch_script", fmt.Sprintf("%d", *job.JobId), "-")
	if err != nil {
		/* If the job has ended some time ago, this will fail.
		 * However, this is not a critical case, so just return an empty job script. */
		return ""
	}
	return stdout
}

func SlurmWarnVersion(ver SlurmMetaSlurmVersion) {
	major, _ := strconv.Atoi(string(ver.Major))
	minor, _ := strconv.Atoi(string(ver.Minor))
	if major < SLURM_MAX_VER_MAJ {
		return
	}
	if major == SLURM_MAX_VER_MAJ && minor <= SLURM_MAX_VER_MIN {
		return
	}
	trace.Warn("Detected Slurm version %s.%s.%s. Last supported version is %d.%d.X. Please check if cc-slurm-adapter is working correctly. If so, bump the version number in the source to suppress this warning.", ver.Major, ver.Minor, ver.Micro, major, minor)
}

func SlurmCheckPerms() {
	trace.Debug("SlurmCheckPerms()")

	/* This function checks, whether we are a Slurm operator. Issue a warning
	 * if we are not. */
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

	SlurmWarnVersion(result.Meta.Slurm.Version)

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

func SlurmGetClusterStats(cluster string) ([]SinfoPartial, error) {
	trace.Debug("SlurmGetClusterStats()")

	stdout, err := callProcess("sinfo", "--cluster", cluster, "--noheader", "--json")
	if err != nil {
		return nil, fmt.Errorf("sinfo on cluster '%s' failed: %w", cluster, err)
	}

	var result SinfoResult
	err = json.Unmarshal([]byte(stdout), &result)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse sinfo JSON: %w", err)
	}

	return result.Sinfo, nil
}

func SlurmParseGRES(gres string) (*GRES, error) {
	// e.g. "gpu:h100:4(IDX:0-3)" --> "gpu" "h100" "4" "IDX" "0-3"
	gresParseRegex := regexp.MustCompile("^(\\w+):(\\w+):(\\d+)\\((\\w+):([0-9,\\-]+)\\)$")
	gresParsed := gresParseRegex.FindStringSubmatch(gres)
	if len(gresParsed) != 6 {
		return nil, fmt.Errorf("Unable to parse GRES: '%s'", gres)
	}

	count, err := strconv.ParseInt(gresParsed[3], 10, 32)
	if err != nil {
		return nil, err
	}

	return &GRES{
		Variant:       gresParsed[1],
		Id:            gresParsed[2],
		Count:         int(count),
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
