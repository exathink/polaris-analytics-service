#!/usr/bin/env bash

YELLOW='\033[0;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
GREEN='\033[0;32m'
MAGENTA='\033[0;35m'
NC='\033[0m'

function message {
    echo -e "${BLUE}$@${NC}"
}

function message2 {
    echo -e "${MAGENTA}$@${NC}"
}

function message3 {
    echo -e "${GREEN}$@${NC}"
}

function warning {
   echo -e "${YELLOW}$1${NC}"
}

function error {
    echo -e "${RED}Error: $1${NC}"
}

function confirm_y_n {
    prompt=$1
    if [[ -z "${prompt}" ]]; then prompt="Confirm "; fi
    if [[ -z "${DONT_PROMPT_FOR_CONFIRMATION}" ]]
    then
        read -p "${prompt}(y/n): " -n 1 -r
        echo
    else
        REPLY='y'
    fi
}
function strip_quotes {
    echo $1 | sed 's/\"//g'

}

function jq_filter {
    declare -a args=("$@")
    program=$1
    JQ_OPTIONS="${args[@]:1}"

    echo "$(jq ${JQ_OPTIONS} -f ${THIS_DIR}/.jq/${program})"
}

function now {
    echo $(date +%s)
}

function get_image_id {
    source_image=$1
    echo "$(docker images -q --filter="reference=${source_image}")"
}

function get_image_sha {
    source_image=$1
    echo "$(docker inspect ${source_image} | jq -r '.[0].Config.Labels["polaris.build.git-sha-short"]')"
}

function get_deployment_image_sha {
    source_image=$1
    if [[ -z "${source_image}" ]]; then source_image="${PACKAGE_DEPLOYABLE_IMAGE}:latest"; fi
    source_image_id="$(get_image_id ${source_image})"
    if [[ ! -z "${source_image_id}" ]]
    then
        image_git_sha="$(get_image_sha ${source_image_id})"
        if [[ ! -z "${image_git_sha}" ]]
        then
            echo "${DOCKER_DEPLOYMENT_REGISTRY}/${PACKAGE_DEPLOYABLE_IMAGE_REPOSITORY}:${image_git_sha}"
        fi
    fi
}
