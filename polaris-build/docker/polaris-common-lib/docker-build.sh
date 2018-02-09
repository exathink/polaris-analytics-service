#!/usr/bin/env bash
function abs_path() {  echo "$(cd $1 && pwd)"; }


repo="polaris-common-lib"
context=$(abs_path ../..)

docker_build.sh ${repo} ${context}

