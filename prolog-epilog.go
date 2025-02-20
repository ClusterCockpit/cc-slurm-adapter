package main

import (
	"fmt"
	"net"
	"os"
	"reflect"
	"encoding/json"
	"slices"

	"github.com/ClusterCockpit/cc-slurm-adapter/trace"
)

/* The following struct *should* contain all environment variables set in the
 * Slurmctld Prologue/Epilogue. See here for the full list:
 * https://slurm.schedmd.com/prolog_epilog.html
 * This may change in Slurm version > 24.11 */
type PrologEpilogSlurmctldEnv struct {
	CUDA_MPS_ACTIVE_THREAD_PERCENTAGE string `json:"CUDA_MPS_ACTIVE_THREAD_PERCENTAGE"`
	CUDA_VISIBLE_DEVICES              string `json:"CUDA_VISIBLE_DEVICES"`
	GPU_DEVICE_ORDINAL                string `json:"GPU_DEVICE_ORDINAL"`
	SLURM_ARRAY_JOB_ID                string `json:"SLURM_ARRAY_JOB_ID"`
	SLURM_ARRAY_TASK_COUNT            string `json:"SLURM_ARRAY_TASK_COUNT"`
	SLURM_ARRAY_TASK_ID               string `json:"SLURM_ARRAY_TASK_ID"`
	SLURM_ARRAY_TASK_MAX              string `json:"SLURM_ARRAY_TASK_MAX"`
	SLURM_ARRAY_TASK_MIN              string `json:"SLURM_ARRAY_TASK_MIN"`
	SLURM_ARRAY_TASK_STEP             string `json:"SLURM_ARRAY_TASK_STEP"`
	SLURM_CLUSTER_NAME                string `json:"SLURM_CLUSTER_NAME"`
	SLURM_JOB_ACCOUNT                 string `json:"SLURM_JOB_ACCOUNT"`
	SLURM_JOB_COMMENT                 string `json:"SLURM_JOB_COMMENT"`
	SLURM_JOB_CONSTRAINTS             string `json:"SLURM_JOB_CONSTRAINTS"`
	SLURM_JOB_CPUS_PER_NODE           string `json:"SLURM_JOB_CPUS_PER_NODE"`
	SLURM_JOB_DERIVED_EC              string `json:"SLURM_JOB_DERIVED_EC"`
	SLURM_JOB_END_TIME                string `json:"SLURM_JOB_END_TIME"`
	SLURM_JOB_EXIT_CODE               string `json:"SLURM_JOB_EXIT_CODE"`
	SLURM_JOB_EXIT_CODE2              string `json:"SLURM_JOB_EXIT_CODE2"`
	SLURM_JOB_EXTRA                   string `json:"SLURM_JOB_EXTRA"`
	SLURM_JOB_GID                     string `json:"SLURM_JOB_GID"`
	SLURM_JOB_GROUP                   string `json:"SLURM_JOB_GROUP"`
	SLURM_JOB_ID                      string `json:"SLURM_JOB_ID"`
	SLURM_JOB_LICENSES                string `json:"SLURM_JOB_LICENSES"`
	SLURM_JOB_NAME                    string `json:"SLURM_JOB_NAME"`
	SLURM_JOB_NODELIST                string `json:"SLURM_JOB_NODELIST"`
	SLURM_JOB_NUM_NODES               string `json:"SLURM_JOB_NUM_NODES"`
	SLURM_JOB_OVERSUBSCRIBE           string `json:"SLURM_JOB_OVERSUBSCRIBE"`
	SLURM_JOB_PARTITION               string `json:"SLURM_JOB_PARTITION"`
	SLURM_JOB_RESERVATION             string `json:"SLURM_JOB_RESERVATION"`
	SLURM_JOB_RESTART_COUNT           string `json:"SLURM_JOB_RESTART_COUNT"`
	SLURM_JOB_START_TIME              string `json:"SLURM_JOB_START_TIME"`
	SLURM_JOB_STDERR                  string `json:"SLURM_JOB_STDERR"`
	SLURM_JOB_STDIN                   string `json:"SLURM_JOB_STDIN"`
	SLURM_JOB_STDOUT                  string `json:"SLURM_JOB_STDOUT"`
	SLURM_JOB_UID                     string `json:"SLURM_JOB_UID"`
	SLURM_JOB_USER                    string `json:"SLURM_JOB_USER"`
	SLURM_JOB_WORK_DIR                string `json:"SLURM_JOB_WORK_DIR"`
	SLURM_SCRIPT_CONTEXT              string `json:"SLURM_SCRIPT_CONTEXT"`
	SLURM_WCKEY                       string `json:"SLURM_WCKEY"`
}

func CollectEnvironmentValues() (PrologEpilogSlurmctldEnv, error) {
	/* Collect all interesting envinroment variables from Slurm and pack them into a struct.
	 * If you need to change any of the enivonrment variables, simply add them to the struct above.
	 * The code below will automatically read the appropriate environment variables.
	 * While Prolog and Epilog use a slightly different set of environment variables, we can
	 * query all of them in both cases. If they are not set, they will simply stay blank, which is okay. */

	var retval PrologEpilogSlurmctldEnv

	// perhaps this reflection part can be done a bit more nicely
	v := reflect.ValueOf(&retval)
	e := v.Elem()
	typeOfs := e.Type()

	for i := 0; i < typeOfs.NumField(); i++ {
		field := e.Field(i)
		if !field.IsValid() || !field.CanSet() || field.Kind() != reflect.String {
			return retval, fmt.Errorf("Field(%d) '%s' must be of type string", i, typeOfs.Field(i).Name)
		}
		e.Field(i).SetString(os.Getenv(typeOfs.Field(i).Name))
	}

	if !slices.Contains([]string{"prolog_slurmctld", "epilog_slurmctld"}, retval.SLURM_SCRIPT_CONTEXT) {
		return retval, fmt.Errorf("Environment variable SLURM_SCRIPT_CONTEXT cotains illegal value: '%s'", retval.SLURM_SCRIPT_CONTEXT)
	}

	return retval, nil
}

func PrologEpilogMain() error {
	trace.Info("Starting Prolog")

	trace.Debug("Connecting to Unix Socket")
	con, err := net.Dial("unix", Config.IpcSockPath)
	if err != nil {
		return fmt.Errorf("Unable to connect to the daemon's unix socket. Is the daemon running?: %w", err)
	}

	trace.Debug("Collecting Environment")
	env, err := CollectEnvironmentValues()
	if err != nil {
		return fmt.Errorf("Unable to collect environment: %w", err)
	}

	j, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("Failed to marshal environment to JSON: %w", err)
	}

	trace.Debug("Sending data to daemon")
	con.Write([]byte(j))
	con.Close()

	return nil
}
