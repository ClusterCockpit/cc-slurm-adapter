package slurm_v24xx

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ClusterCockpit/cc-slurm-adapter/internal/slurm/common"
)

type resourceSqueueConfig struct {
	CallsPath        string
	Output           string
	QueryExitCode    int
	FallbackExitCode int
}

func TestMain(m *testing.M) {
	// Reuse the test binary as squeue so all mock behavior stays in Go.
	if filepath.Base(os.Args[0]) == "squeue" {
		os.Exit(runResourceSqueue())
	}
	os.Exit(m.Run())
}

func runResourceSqueue() int {
	data, err := os.ReadFile(os.Getenv("CC_SLURM_ADAPTER_TEST_SQUEUE_CONFIG"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	var config resourceSqueueConfig
	if err := json.Unmarshal(data, &config); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	calls, err := os.OpenFile(config.CallsPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	_, writeErr := fmt.Fprintln(calls, strings.Join(os.Args[1:], " "))
	closeErr := calls.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	exitCode := config.FallbackExitCode
	if slices.Contains(os.Args[1:], "-j") {
		exitCode = config.QueryExitCode
	}
	if exitCode == 0 {
		fmt.Print(config.Output)
	}
	return exitCode
}

func mockResourceSqueue(t *testing.T, output string, queryExitCode, fallbackExitCode int) string {
	t.Helper()
	dir := t.TempDir()
	calls := filepath.Join(dir, "calls")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(executable, filepath.Join(dir, "squeue")); err != nil {
		t.Fatal(err)
	}
	config := resourceSqueueConfig{
		CallsPath: calls, Output: output,
		QueryExitCode: queryExitCode, FallbackExitCode: fallbackExitCode,
	}
	data, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "squeue.json")
	if err := os.WriteFile(configPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	t.Setenv("CC_SLURM_ADAPTER_TEST_SQUEUE_CONFIG", configPath)
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
	calls := mockResourceSqueue(t, `{"jobs":[]}`, 1, 0)
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
	calls := mockResourceSqueue(t, `{"jobs":[{"job_id":202,"cluster":"example"},{"job_id":999,"cluster":"example"}]}`, 1, 0)
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
	calls := mockResourceSqueue(t, "", 1, 1)
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
	calls := mockResourceSqueue(t, `{"jobs":[{"job_id":202,"cluster":"example"}]}`, 0, 1)
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
