#!/bin/sh

set -eu

DIR="$(dirname "$(readlink -f "$0")")"

env | sort > /tmp/mylog.env

"${DIR}/cc-slurm-adapter" -debug 5 2> /tmp/mylog
