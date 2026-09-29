package slurm_v24xx

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ClusterCockpit/cc-slurm-adapter/internal/slurm/common"
)

func mockResourceSqueue(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	calls := filepath.Join(dir, "calls")
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$SQUEUE_CALLS\"\n" + body
	if err := os.WriteFile(filepath.Join(dir, "squeue"), []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	t.Setenv("SQUEUE_CALLS", calls)
	return calls
}

func assertResourceSqueueCalls(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("squeue calls:\n%s\nwant:\n%s", got, want)
	}
}

func TestResourceQueryFallsBackForExpiredJob(t *testing.T) {
	calls := mockResourceSqueue(t, `case " $* " in
 *" -j "*) exit 1 ;;
esac
printf '%s' '{"jobs":[]}'
`)
	id, cluster := int64(101), "example"
	accounting := &SacctJob{JobId: &id, Cluster: &cluster}
	job := &Job{sa: accounting}
	api := slurmApi{}
	if err := api.QueryJobsWithResources(cluster, []slurm_common.Job{job}); err != nil {
		t.Fatal(err)
	}
	if job.sa != accounting || job.sc != nil {
		t.Fatal("lost accounting data or fabricated controller data")
	}
	assertResourceSqueueCalls(t, calls, "--noheader --cluster example -j 101 --json\n--noheader --cluster example --all --json\n")
}

func TestResourceQueryFallbackPreservesMixedJobs(t *testing.T) {
	calls := mockResourceSqueue(t, `case " $* " in
 *" -j "*) exit 1 ;;
esac
printf '%s' '{"jobs":[{"job_id":202,"cluster":"example"},{"job_id":999,"cluster":"example"}]}'
`)
	expiredID, runningID, cluster := int64(101), int64(202), "example"
	expiredAccounting := &SacctJob{JobId: &expiredID, Cluster: &cluster}
	runningAccounting := &SacctJob{JobId: &runningID, Cluster: &cluster}
	expired, running := &Job{sa: expiredAccounting}, &Job{sa: runningAccounting}
	jobs := []slurm_common.Job{expired, running}
	api := slurmApi{}
	if err := api.QueryJobsWithResources(cluster, jobs); err != nil {
		t.Fatal(err)
	}
	if expired.sa != expiredAccounting || expired.sc != nil {
		t.Fatal("expired job lost accounting data or acquired an unrelated allocation")
	}
	if running.sa != runningAccounting || running.sc == nil || *running.sc.JobId != runningID {
		t.Fatal("running job did not retain accounting data and receive its allocation")
	}
	if len(jobs) != 2 {
		t.Fatal("fallback added an unrequested job")
	}
	assertResourceSqueueCalls(t, calls, "--noheader --cluster example -j 101,202 --json\n--noheader --cluster example --all --json\n")
}

func TestResourceQueryFallbackFailureIsReturned(t *testing.T) {
	calls := mockResourceSqueue(t, "exit 1\n")
	id, cluster := int64(101), "example"
	job := &Job{sa: &SacctJob{JobId: &id, Cluster: &cluster}}
	api := slurmApi{}
	err := api.QueryJobsWithResources(cluster, []slurm_common.Job{job})
	if err == nil || !strings.Contains(err.Error(), "Unable to query squeue resources") {
		t.Fatalf("expected fallback error, got %v", err)
	}
	assertResourceSqueueCalls(t, calls, "--noheader --cluster example -j 101 --json\n--noheader --cluster example --all --json\n")
}

func TestResourceQuerySuccessfulLookupDoesNotFallBack(t *testing.T) {
	calls := mockResourceSqueue(t, `printf '%s' '{"jobs":[{"job_id":202,"cluster":"example"}]}'`)
	id, cluster := int64(202), "example"
	job := &Job{sa: &SacctJob{JobId: &id, Cluster: &cluster}}
	api := slurmApi{}
	if err := api.QueryJobsWithResources(cluster, []slurm_common.Job{job}); err != nil {
		t.Fatal(err)
	}
	if job.sc == nil || *job.sc.JobId != id {
		t.Fatal("missing allocation after successful lookup")
	}
	assertResourceSqueueCalls(t, calls, "--noheader --cluster example -j 202 --json\n")
}
