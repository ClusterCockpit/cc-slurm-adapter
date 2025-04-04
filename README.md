# cc-slurm-adapter

## Overview

cc-slurm-adapter is a software daemon, which feeds [cc-backend](https://github.com/ClusterCockpit/cc-backend) with job information from [Slurm](https://slurm.schedmd.com/) in realtime.
It is designed to be reasonably fault tolerant and verbose (if enabled) about possible undesired conditions.
For example, should cc-backend go down or Slurm be unavailable, cc-slurm-adapter should handle everything as expected.
That means that no jobs are lost and they are submitted to cc-backend as soon as everything is running again.
This also includes cc-slurm-adapter itself.
So if cc-slurm-adapter should encounter an internal error (e.g. incompatibility due to new Slurm version), no jobs should be lost.
However, there are exceptions.
Please consult the Limitations section for more details.

This daemon runs on the same node, which [slurmctld](https://slurm.schedmd.com/slurmctld.html) runs on.
Data is obtained via Slurm's commands `sacct`, `squeue`, `sacctmgr`, and `scontrol`.
`slurmrestd` is not used, thus not required.
However, the usage of Slurm's slurmdbd is mandatory.

When the daemon runs, a periodic timer (default 1 minute) triggers a synchronization between the state of Slurm and cc-backend.
Data is then submitted to cc-backend via REST API.
If the job was successfully started or stopped, a notification can optionally be sent over [NATS](https://nats.io/) to notify any kind of client about changes.

In addition to the regular periodic timer, cc-slurm-adapter can be set up to immediately trigger upon a [Slurm Prolog/Epilog](https://slurm.schedmd.com/prolog_epilog.html).
This allows the job to be submitted more or less immediately to cc-backend and significantly reduces the delay.
However, the usage of slurmctld's prolog/epilog hook is optional and is only useful to reduce update delay.

## Limitations

Because slurmdbd does not appear to store all information about a job, submitted jobs to cc-backend may lack specific information in certain cases.
This includes all the information, that are not obtainable via a `sacct`.
For example, cc-slurm-adapter obtains resource allocation information via `scontrol --cluster XYZ show job XYZ --json`.
However, it appears that this resource information becomes unavailable after a few minutes once the job has stopped (regardless of success or failure).
Should the resource allocation information be of importance, one must make sure the daemon is not stopped for too long.
Most notably, should resource information be unavailable, cc-backend cannot associate the job with metrics anymore.
The jobs can still be listed in cc-backend, but showing CPU, GPU, and memory metrics won't work anymore.

## Slurm Version Compatibility

### Slurm Related Code

We try to keep all Slurm related code in `slurm.go`.
This most notably refers to the JSON structure, which is returned by the various Slurm commands.
The most likely error that you will encounter with a Slurm incompatibility is a nil pointer dereference.
While this currently may happen outside of `slurm.go`, the line should more or less uniquely identify which field is missing.

### Slurm JSON

All Slurm JSON structs are replicated using Go structs and members are declared as pointer type.
That way we actually know if a value was missing during JSON parsing.
Should you encounter the situation of a nil dereference, use the following commands to check against Slurm's current JSON layout:

- Get a any job IDs either via `squeue` or `sacct`.
- Unfortunately `scontrol` returns a different JSON layout than `sacct`. Accordingly, we need both JSON layouts returned by the following two commands:
  - `sacct -j 12345 --json`, where 12345 is the job's ID.
  - `scontrol show job 12345 --json`, where 12345 is the job's ID.

### SlurmInt and SlurmString in JSON

At some point Slurm started transitioning from plain integers to a struct, which contains whether the integer is infinite or actually set at all.
It appears that with every version change, more values in the JSON change from plain integers to this integer struct.
When you encounter such a change, use the custom type `SlurmInt`, which supports being parsed as plain integer and integer struct.
That way we can keep compatibility with old versions.

A similar situation exists with strings.
For some reason, Slurm started replacing some strings in the API with arrays, which may contain an arbitrary amount of strings (possibly none at all).
Because the makes handling the API rather nasty, the SlurmString type can be used.
This can be parsed both as a normal string and such an array.
Should the array be empty, it is interpreted as a blank string.
Should the array have at least one value, the string is set to that particular value.
While there may be the case of multiple values in an array, we have not encountered this under normal circumstances.
Though, it is possible this may change in newer Slurm versions.

## Command Line Usage

Options/Argument | Description
--- | ---
`-config <path>`        | Specify the path to the config file
`-daemon`               | Run the cc-slurm-adapter in daemon mode
`-debug <log-level>`    | Set the log evel (default is 2)
`-help`                 | Show help for all command line flags

If `-daemon` is not supplied, cc-slurm-adapter runs in Prolog/Epilog mode.
This only works when running from a Slurm Prolog/Epilog context.

## Configuration

### Example

Here is an example of the configuration file.
Most values are optional, see Reference to see which ones you really need.

```json
{
    "pidFilePath": "/run/cc-slurm-adapter/daemon.pid",
    "ipcSockPath": "/run/cc-slurm-adapter/daemon.sock",
    "lastRunPath": "/var/lib/cc-slurm-adapter/last_run",
    "slurmPollInterval": 60,
    "slurmQueryDelay": 1,
    "slurmQueryMaxSpan": 604800,
    "slurmQueryMaxRetries": 5,
    "ccRestUrl": "https://my-cc-backend-instance.example",
    "ccRestJwt": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    "gpuPciAddrs": {
        "^nodehostname0[0-9]$": [
            "00000000:00:10.0",
            "00000000:00:3F.0"
        ],
        "^nodehostname1[0-9]$": [
            "00000000:00:10.0",
            "00000000:00:3F.0"
        ]
    },
    "ignoreHosts": "^nodehostname9\\w+$",
    "natsServer": "mynatsserver.example",
    "natsPort": 4222,
    "natsSubject": "mysubject",
    "natsUser": "myuser",
    "natsPassword": "123456789",
    "natsCredsFile": "/etc/cc-slurm-adapter/nats.creds",
    "natsNKeySeedFile": "/etc/ss-slurm-adapter/nats.nkey"
}
```

### Reference

For example values of each config key, check the example above.
The non-optional values above show the default values.

Config Key | Optional | Description
--- | --- | ---
`pidFilePath`       | yes | Path to the PID file. cc-slurm-adapter manages its own PID file in order to avoid concurrent execution.
`ipcSockPath`       | yes | Path to the IPC socket. This socket is needed for cc-slurm-adapter running in prolog/epilog to communicate to cc-slurm-adapter running in daemon mode.
`lastRunPath`       | yes | Path to the file which contains the time stamp of cc-slurm-adapter's last successful sync to cc-backend. Time is stored as a file timestamp, not in the file itself.
`slurmPollInterval` | yes | Interval (seconds) in which a sync to cc-backend occurs, assuming no prolog/epilog event occurs.
`slurmQueryDelay`   | yes | Time (seconds) to wait between prolog/epilog event to actual synchronization. This is just for good measure to give Slurm some time to react. There should usually be no need to change this.
`slurmQueryMaxSpan` | yes | Maximum time (seconds) cc-slurm-adapter is allowed to synchronize jobs from the past. This is to avoid accidental flooding with millions of jobs from e.g. multiple years.
`slurmMaxRetries`   | yes | Maximum attempts Slurm should be queried upon a Prolog/Epilog event. If Slurm is reacting slow or isn't available at all, this limits the "fast" attempts to query Slurm about that job. Even if it should time out, a later occuring synchronize should still catch this job. Though, the latency from Slurm to cc-backend is increased. There should usually be no need to change this.
`ccRestUrl`         | no  | The URL to cc-backend's REST API. Must not contain a trailing slash.
`ccRestJwt`         | no  | The JWT obtained from cc-backend, which allows access to the REST API.
`gpuPciAddrs`       | yes | Dictionary of Regexes mapping to a list of PCI addresses. If some of your nodes have GPUs, use this to map the hostnames via regex to a list of GPUs those nodes have. They have to be ordered like NVML shows them, which should hopefully be the same as `nvidia-smi` shows them, if all devices are visible.
`ignoreHosts`       | yes | Regex of hostnames to ignore. If all hosts that are associated to a job match this regex, the job is discarded and not reported to cc-backend.
`natsServer`        | yes | Hostname of the NATS server. Leave blank or omit to disable NATS.
`natsPort`          | yes | Port of the NATS server.
`natsSubject`       | yes | Subject to which publish job information to.
`natsUser`          | yes | If your NATS server requires [user auth](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/auth_intro/username_password), specify NATS user.
`natsPassword`      | yes | Password to be used with the NATS user.
`natsCredsFile`     | yes | If your NATS server requires a [credentials file](https://docs.nats.io/using-nats/developer/connecting/creds), use this to set the file path.
`natsNKeySeedFile`  | yes | If your NATS server requires plain [NKey auth](https://docs.nats.io/running-a-nats-service/configuration/securing_nats/auth_intro/nkey_auth), use this to specify the path to the file, which contains the NKey seed (private key).

## Admin Guide

### How to compile

```
$ go build
```

### Daemon (required)

#### Copy binary and configuration

This should be self explanatory.
You can copy the binary and the configuration anywhere you like.
Since the configuration file contains the JWT for cc-backend and possibly NATS credentials, make sure to use appropriate file permissions.

#### Installing the service

See this example systemd service file:

```ini
[Unit]
Description=cc-slurm-adapter

Wants=network.target
After=network.target

[Service]
User=cc-slurm-adapter
Group=slurm
ExecStart=/opt/cc-slurm-adapter/cc-slurm-adapter -daemon -config /opt/cc-slurm-adapter/config.json
WorkingDirectory=/opt/cc-slurm-adapter/
RuntimeDirectory=cc-slurm-adapter
RuntimeDirectoryMode=0750
Restart=on-failure
RestartSec=15s

[Install]
WantedBy=multi-user.target
```

This service file runs the cc-slurm-adapter daemon as the user `cc-slurm-adapter`.
A runtime directory /run/cc-slurm-adapter is created for the PID file and IPC socket.
The group is set to `slurm` so that the RuntimeDirectoryMode=0750 will allow the group `slurm` to enter this directory.
Access from the `slurm` group is only necessary, if the slurmctld Prolog/Epilog hook is used.
That is because the Prolog/Epilog is executed as the slurm user/group and otherwise cc-slurm-adapter (in Prolog/Epilog mode) is unable to open the Unix socket in the runtime directory.

#### Setting cc-slurm-adapter user Slurm permissions

Depending on the Slurm configuration, an arbitrary user may not be allowed to access information from Slurm via `sacct` or `scontrol`.
Since it is recommended to run cc-slurm-adapter as its own user, this user (e.g. `cc-slurm-adapter`) needs to be given permission.
You can do this like the following:

```
$ sacctmgr add user cc-slurm-adapter Account=root AdminLevel=operator
```

If your Slurm instance is restricted and the permissions are not given, NO JOBS WILL BE REPORTED!

#### Debugging the daemon

Okay, so you have set up the daemon, but no jobs are running or it is crashing?
In that case you should first check the log.
cc-slurm-adapter doesn't have its own log file.
Instead it prints all errors and warnings to stderr.

If the log doesn't show anything useful, you can increase the default log-level from 2 to 5 via `-log-level 5`.
While it may spam the console for many jobs, the debug messages may give insight to what exactly is going wrong.
Though, cc-slurm-adapter attempts to print anything of significance to the default log level.

### slurmctld Prolog/Epilog Hook (optional)

If you want to make cc-slurm-adapter more responsive to new jobs, you can enable the slurmctld hook.
In your `slurm.conf`, add the following lines:

```ini
PrEpPlugins=prep/script
PrologSlurmctld=/some_path/hook.sh
EpilogSlurmctld=/some_path/hook.sh
```

Where `hook.sh` is an executable shell script which may look like this:

```bash
#!/bin/sh

/opt/cc-slurm-adapter/cc-slurm-adapter

exit 0
```

Generally speaking, it is not necessary to specify the config path during the Prolog/Epilog invocation.
However, that is only the case if the default IPC socket path `/run/cc-slurm-adapter/daemon.sock` is used.
If you want to change that, you have to add `-config /some_other_path/config.json` to your `hook.sh` script.
In that case the config also has to be readable by the user or group `slurm`.

It is important to exit the script with 0.
That is because the exit code of a Slurm Prolog/Epilog determines whether a job allocation should succeed or not.
While this allocation failure can be useful for ensuring correct operation, it should not be used in production.
If for example the cc-slurm-adapter is restarterd or stopped for some time, the Prolog/Epilog calls will fail and will immediately deny all job allocations.
This is most certainly undesirable in a production instance.
