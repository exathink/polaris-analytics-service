#!/usr/bin/env bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source ${THIS_DIR}/utils.sh

function create_package {
    if [[ $# < 3 ]]
    then
        message \
    "Usage: Please create an empty git repository at <remote-repository> and then run

                package scaffold package <local_path> <remote_repository_name> <remote_repository_group>

    The package template will be cloned to <local_path>
    The remote repository will be made its new default remote with name 'gitlab' and the clone will be pushed to it"
        exit 1
    fi

    local_path="$1"
    remote_repository_name="$2"
    remote_repository_group="$3"


    TARGET_DIR="$(cd "${local_path}"; pwd)/${remote_repository_name}"
    message "Cloning repository: git@gitlab.com:/polaris-common/polaris-package-template.git"
    git clone git@gitlab.com:/polaris-common/polaris-package-template.git  "${TARGET_DIR}"
    if [[ $? -eq 0 ]]
    then
        pushd . &>/dev/null
        cd ${TARGET_DIR}
        message "Working directory: ${TARGET_DIR}"
        message "Initializing submodules.."
        git submodule update  --init --remote
        polaris-build/scripts/sync_submodule_commits.sh
        message "Removing remote: origin: $(git remote get-url --all origin)"
        git remote remove origin
        message "Adding remote gitlab: git@gitlab.com:${remote_repository_group}/${remote_repository_name}.git"
        git remote add "gitlab" "git@gitlab.com:${remote_repository_group}/${remote_repository_name}.git"
        message "pushing master to remote"
        git push --set-upstream gitlab master
        popd &>/dev/null
        initialized=true
        message "Package  at ${TARGET_DIR} is ready."
    else
        error "Package clone failed with error $?"
        exit $?
    fi
}

function add_submodule {
    submodule="$1"
    if [[ $? < 1 ]]; then error "Usage: submodule add <remote>"; exit 1; fi

    message "adding submodule ${submodule}"
    git submodule add "../../${submodule}.git"

    if [[ $? -eq 0 ]]
    then
        git ct -m "Added submodule: ${submodule}"
    fi
}

function remove_submodule {
    submodule=$1

    git rm "${submodule}"
    rm -rf .git/modules/${submodule}
}


function submodules_cmd {
    command=$1
    declare -a args=("$@")
    command_args="${args[@]:1}"
    case ${command} in
        add)
            add_submodule ${command_args}
            ;;
        remove | rm)
            remove_submodule ${command_args}
            ;;
        *)
            error "Unknown submodule command ${command}"
            exit 1
            ;;
    esac
}

function migrations_cmd {
    # copy the files and directories needed to support alembic migrations
    SCAFFOLD_DIR="${BASEDIR}/polaris-build/scaffold"
    cp -r "${SCAFFOLD_DIR}/migrations" "${BASEDIR}"
    cp "${SCAFFOLD_DIR}/alembic.ini" "${BASEDIR}"
}

function scaffold_cmd {
    command=$1
    declare -a args=("$@")
    command_args="${args[@]:1}"

    case ${command} in
        package)
            create_package ${command_args}
            ;;
        submodule)
            submodules_cmd ${command_args}
            ;;
        migrations)
            migrations_cmd ${command_args}
            ;;
        *)
            error "Unknown scaffold command: ${command}"
            ;;
    esac
}