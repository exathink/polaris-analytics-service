#!/usr/bin/env bash
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source "${THIS_DIR}/aws.sh"
source "${THIS_DIR}/github.sh"
source "${THIS_DIR}/gitlab.sh"


function scm_cmd {
    declare -a args=("$@")
    command=$1
    scm=$2
    COMMAND_ARGS="${args[@]:2}"

    case ${command} in
        update-ssh-key)
            case ${scm} in
                github)
                    github_update_ssh_public_key ${COMMAND_ARGS}
                    ;;
                gitlab)
                    gitlab_update_ssh_public_key ${COMMAND_ARGS}
                    ;;
                *)
                    error "Unknown scm target ${scm}"
                    exit 1
                    ;;
            esac
            ;;
        *)
            error "Unknown scm command ${command}"
            ;;
    esac
}