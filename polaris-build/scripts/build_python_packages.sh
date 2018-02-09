#!/usr/bin/env bash
set -e
function list_submodules {
    if [[ -f .gitmodules ]]
    then
        echo "$(grep path .gitmodules | sed 's/.*= //')"
    fi
}

function build_package {
    declare visited=$1

    for submodule in $(list_submodules); do
        if [[ "${submodule}" != "polaris-build" ]]
        then
            if [[ ! -f "${visited}/${submodule}" ]]
            then
                touch "${visited}/${submodule}"
                pushd . >&/dev/null
                cd "${submodule}"
                build_package ${visited}
                popd >&/dev/null
            fi
        fi
    done
    if [ -f setup.py ]
    then
            echo "Processing: ${PWD}"
            if [[ ${INSTALL_MODE} == "--dependencies-only"  ]]
            then
                if [[ -f requirements.txt ]]
                then
                    echo "-----------------------Install dependencies for python package: ${PWD}-----------------------------------"
                    pip wheel  -w /project/wheels --find-links /project/wheels -c ${POLARIS_BUILD}/system-pip-constraints.txt -r requirements.txt
                    echo "pip install: ${PWD}"
                    pip install --no-index --find-links /project/wheels -c ${POLARIS_BUILD}/system-pip-constraints.txt -r requirements.txt
                    echo "-------------------------Finished installing dependencies for python package: ${PWD}---------------------"
                fi
            else
                echo "pip install: ${PWD}"
                pip install  -c ${POLARIS_BUILD}/system-pip-constraints.txt .
                echo "${PWD}" >> /polaris-build-info/installed_module_paths.txt
                echo "-------------------------Finished building python package: ${PWD}---------------------"
           fi
    fi
}

if [ ! -z "$1" ]
then

    INSTALL_ROOT=$1
    INSTALL_MODE=$2
    POLARIS_BUILD="${INSTALL_ROOT}/polaris-build"
    echo "Install mode: ${INSTALL_MODE}"
    mkdir -p /polaris-build-info

    visited=$(mktemp -d)
    build_package ${visited}
    ls -D ${visited} > /polaris-build-info/installed_modules.txt
    rm -rf ${visited}
else
    echo "build_python_packages.sh must be passed the installation directory as an argument"
    exit 1
fi

