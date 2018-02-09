#!/usr/bin/env bash


THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source ${THIS_DIR}/utils.sh



function make_submodule_path_expression {
    # This function creates a python path expression by concatenating the paths to each of the submodules
    # reachable from this repository. All the polaris modules that the current package depends on should now on
    # python path.
    TEMPFILE=$(mktemp)
    # Step 1: Write all the paths to the submodules descendant from this directory to TEMPFILE.
    git submodule foreach --recursive "echo \$toplevel/\$path >> ${TEMPFILE}" > /dev/null
    # Step 2: First sed expression turns space separated path into a : separated path so that it can be a PYTHON path.
    # Step 3: Since container paths are relative to /project which these paths are generated on the host and relative to $PWD
    #         the second sed expression replaces ${PWD} with /project in all paths.
    export SUBMODULE_PATHS="$(echo $(cat ${TEMPFILE}) | sed 's/ /:/g' |sed "s/$(echo $PWD | sed 's/\//\\\//g')/\/project/g")"
    rm ${TEMPFILE}
}

function build_python_path {
    declare parent_path=$1
    declare parent_project_path=$2
    python_path=$3
    visited=$4
    for submodule in $(list_submodules); do
        if [[ "${submodule}" != "polaris-build" &&  ! -f ${visited}/${submodule} ]]
        then
            touch ${visited}/${submodule}

            python_path="${python_path}:${parent_project_path}/${submodule}"

            pushd . &>/dev/null
            cd ${parent_path}/${submodule}
            build_python_path "${parent_path}/${submodule}" "${parent_project_path}/${submodule}" ${python_path} ${visited}
            popd &>/dev/null
        fi
    done

}

function set_python_path {
    declare parent_path=${BASEDIR}
    declare parent_project_path="/project"
    declare python_path="/project"
    declare visited=$(mktemp -d)

    build_python_path  "${parent_path}"  "${parent_project_path}" "${python_path}" ${visited}
    export PYTHONPATH="${python_path}:${PYTHONPATH}"
    rm -rf visited
}


function _submodule_update_dfs {
    declare visited=$1
    declare path=$2
    declare checkout_branch=$3
    if [[ -z "$path" ]]; then message "Updating ${REPO_NAME}"; else message "Updating ${REPO_NAME}${path}"; fi
    for submodule in $(list_submodules); do
        if [[ "${submodule}" != "polaris-build" ]]
        then
            if [[ ! -f "${visited}/${submodule}" ]]
            then
                touch "${visited}/${submodule}"
                git submodule update --init --remote  ${submodule}
                pushd . &>/dev/null
                cd ${submodule}
                if [[ ! -z "${checkout_branch}" ]]
                then
                    git checkout ${checkout_branch}
                    git pull
                fi
                _submodule_update_dfs ${visited} "${path}/${submodule}" "${checkout_branch}"
                popd &>/dev/null
            fi
        fi
    done

}

function build_package_dependencies {
    tmp=$(mktemp -d)
    docker run ${PACKAGE_IMAGE} cat /polaris-build-info/installed_modules.txt > "${tmp}/installed_modules.txt"
    docker run ${PACKAGE_IMAGE} cat /polaris-build-info/installed_module_paths.txt > "${tmp}/installed_module_paths.txt"
    dependencies="["
    count=0
    for package in $(cat "${tmp}/installed_modules.txt" ); do
       if [[ ${count} -gt 0 ]]; then dependencies="${dependencies}, "; fi

       install_path="$(cat "${tmp}/installed_module_paths.txt" | grep -e "${package}$" | sed 's/\/project\///')"
       if [[ -d "${install_path}" ]]
       then
           pushd . &>/dev/null
           cd "${install_path}"
           dependencies="${dependencies}{ \"package\": \"${package}\", \"git-sha\":  \"$(git rev-parse HEAD)\", \"git-sha-short\":  \"$(git rev-parse --short HEAD)\",  \"git-ref\": \"$(git rev-parse --abbrev-ref HEAD)\"}"
           popd &>/dev/null
           count=$((count+1))
       else
           error "Install path ${install_path} could not be found when building package dependencies"
           exit 1
       fi
    done
    dependencies="${dependencies}]"

    echo "${dependencies}"

}

function show_image_package_dependencies {
   image="$1"
   echo "$(echo "$(docker inspect ${image} | jq '.[0].Config.Labels["polaris.build.package-dependencies"]' | jq -r '.')" | jq '.')"

}

function submodule_deinit {
    for submodule in $(git submodule foreach --quiet 'echo $name'); do
        if [[ "${submodule}" != "polaris-build" ]]
        then
            message "de-init: ${submodule}"
            git submodule deinit -f "${submodule}"
        fi
    done

}

function submodule_update {
    visited=$(mktemp -d)
    if [[ "$1" == "clean" || "$1" == "checkout-clean" ]]
    then
        submodule_deinit
    fi
    if [[ "$1" == "checkout" || "$1" == "checkout-clean" ]]
    then
        checkout_branch="$2"
    fi
    if [[ -d 'polaris-build'  && ! -z "${checkout_branch}" ]]
    then
        message "Updating polaris-build"
        git submodule update --init --remote polaris-build
        pushd . &>/dev/null
        cd polaris-build
        git checkout ${checkout_branch}
        git pull
        popd &>/dev/null
    fi
    _submodule_update_dfs ${visited} "" ${checkout_branch}
    rm -rf ${visited}
}

function changes {
    for changed in $(git diff --name-only); do
        is_submodule=""
        for submodule in $(git submodule foreach --quiet "echo \$name"); do

            if [[ "${submodule}" == "${changed}" ]]
            then
                is_submodule=1
            fi
         done
         if [[ -z "${is_submodule}" ]]; then echo "* $changed"; fi
    done
    git submodule foreach --recursive --quiet 'for changed in $(git diff --name-only); do
                                                    is_submodule=""
                                                    for submodule in $(git submodule foreach --quiet "echo \$name"); do

                                                        if [[ "${submodule}" == "${changed}" ]]
                                                        then
                                                            is_submodule=1
                                                        fi
                                                     done
                                                     if [[ -z "${is_submodule}" ]]; then echo "-> $path/$changed"; fi
                                                done'
}

function run {
   args="$@"
   set_python_path
   ${DOCKER_COMPOSE} run --rm --no-deps ${args}
}

function check_reqs_txt {
    if [[ ! -z "${ALL_REQUIREMENTS_FILTER}" ]]
    then
        docker run --rm ${PACKAGE_IMAGE} pip freeze | grep -v ${ALL_REQUIREMENTS_FILTER} > all-requirements.txt
    else
        docker run --rm ${PACKAGE_IMAGE} pip freeze > all-requirements.txt
    fi
    if [[ -z "$(git ls-files all-requirements.txt)" ]]
    then
        git add all-requirements.txt
        warning "Please note: all-requirements.txt has been added to version control.  Please commit this change."
    else
        if [[ ! -z "$(git diff all-requirements.txt)" ]]
        then
            warning "Please note: all-requirements.txt has been updated. Please commit this change."
        fi
    fi
}

function build_deployable_images {
    if [[ ! -z "${PACKAGE_DEPLOYABLE_BUILD_TARGETS}" &&  ! -z "${PACKAGE_DEPLOYABLE_IMAGE}" ]]
    then
       message "Building deployable image: ${PACKAGE_DEPLOYABLE_IMAGE}"
       for target in ${PACKAGE_DEPLOYABLE_BUILD_TARGETS}; do
            message "..Building target: ${target}"
            ${DOCKER_COMPOSE} build ${target}
            if [[ $? > 0 ]]; then error 'build package failed.'; exit $?; fi
       done
       label_image ${PACKAGE_DEPLOYABLE_IMAGE}
    fi
}

function build_package {

    message "Building package base image: ${PACKAGE_BASE_IMAGE} with Builder Image: ${BUILDER_IMAGE} and Release Image: ${RELEASE_IMAGE}"
    ${DOCKER_COMPOSE} build package-base
    if [[ $? > 0 ]]; then error 'build package-base failed.'; exit $?; fi


    message "Building package image: ${PACKAGE_IMAGE}"
    ${DOCKER_COMPOSE} build package
    if [[ $? > 0 ]]; then error 'build package failed.'; exit $?; fi
    label_image ${PACKAGE_IMAGE}

    build_deployable_images

    check_reqs_txt
}



function build_all {
    for service in $(${DOCKER_COMPOSE} config --services); do
        echo ${service}
        build ${service}
    done
}

function build {
    if [[ -z "$1" ]]
    then 
        build_package
    else
        case $1 in
        deployable)
            build_deployable_images
            ;;
        *)
            message "Building image for $1"
            ${DOCKER_COMPOSE} build $1
            ;;
        esac
    fi
}

function update_base_image {
    mkdir -p "/tmp/cid"
    CIDFILE="/tmp/cid/${REPO_NAME}-$(date +"%s")"
    message "Updating dependencies in base image."
    docker run --cidfile ${CIDFILE} -v ${BASEDIR}:/src -w /src ${PACKAGE_BASE_IMAGE} /src/polaris-build/scripts/update_python_dependencies.sh
    docker commit `cat ${CIDFILE}` ${PACKAGE_BASE_IMAGE}:latest
    docker rm  -f `cat ${CIDFILE}`
    rm ${CIDFILE}
    message "Base image updated"
}

function update {
    update_base_image
    if [[ "$1" == 'all' ]]
    then
        message "Building package image: ${PACKAGE_IMAGE}"
        ${DOCKER_COMPOSE} build package
    fi
}

function test_setup {
    if [[ -f ${BASEDIR}/test/hooks/test_setup.sh ]]
    then
      message "Running test setup in ${REPO_NAME}/test/hooks/test_setup.sh"
      ${BASEDIR}/test/hooks/test_setup.sh
    fi
}

function test_teardown {
    TEARDOWNS_PENDING=""
    if [[ -f ${BASEDIR}/test/hooks/test_teardown.sh ]]
    then
      message "Running test teardown in ${REPO_NAME}/test/hooks/test_teardown.sh"
      ${BASEDIR}/test/hooks/test_teardown.sh
    else
        down
    fi
}

function run_tests {
    set_python_path
    ${DOCKER_COMPOSE} run --no-deps --rm package-base "$@"
}

function test_package_base {
    set_python_path
    test_setup
    ${DOCKER_COMPOSE} run package-base "$@"
    test_teardown
}

function dump_container_logs {
    container_name=$1
    container_id="$(get_container_id ${container_name})"
    if [[ ! -z "${container_id}" ]]
    then
        docker logs ${container_id}
    fi
}

function get_container_id {
    container_name=$1
    echo "$(docker ps -a -q --filter "name=${container_name}")"
}

function test_package {
    TEARDOWNS_PENDING=true
    test_setup


    if [[ -f ${BASEDIR}/test/hooks/run_tests.sh ]]
    then
        message "Running custom test runner in ${REPO_NAME}/test/hooks/run_tests.sh"
        ${BASEDIR}/test/hooks/run_tests.sh
    else
        message "Running tests using default package test runner"
        container_name="${REPO_NAME}_test-$(date +"%s")"
        ${DOCKER_COMPOSE} run --name  ${container_name} package
        tests_exit_code=$?
        container_id=$(get_container_id ${container_name})
    fi
    if [[ ${DUMP_TEST_LOGS_TO_STDOUT} ]]
    then
        message "Test Execution Summary"
        docker logs ${container_id}
    fi

    if [[ ${tests_exit_code} != 0 && ${tests_exit_code} != 5 ]]
    then
     error "Test run exited with code ${tests_exit_code}."
     docker rm -f ${container_id} >/dev/null
     test_teardown
     exit ${tests_exit_code}
    else
     if [[ ${tests_exit_code} == 5 ]]; then warning " No tests were collected or run."; fi
     docker rm -f ${container_id} >/dev/null
     test_teardown
    fi
}

function down {
    ${DOCKER_COMPOSE} down $1
}

function up {
    task=$1
    set_python_path
    ${DOCKER_COMPOSE} up -d ${task}
}

function images {
    option=$1
    case ${option} in

       --repo-name)
            docker images --filter="reference=*${REPO_NAME}*"
        ;;
        *)
        # Lists all images created for this package
        docker images --filter="label=polaris.build.source_repo=${REPO_NAME}" | grep -v '<none>'
        ;;
    esac
}

function installed {
    case $1 in
        --base)
            docker run --rm -it ${PACKAGE_BASE_IMAGE} pip freeze
            ;;
        --filter)
            if [[ -z "$2" ]]; then error "Usage: package installed --filter <regular-expression>"; fi
            docker run --rm -it ${PACKAGE_IMAGE} pip freeze | grep "$2"
            ;;
         *)
            docker run --rm -it ${PACKAGE_IMAGE} pip freeze
            ;;
    esac
}

function logs {
   service=$1
   ${DOCKER_COMPOSE} logs ${service}
}

function ps {
   service=$1
   if [[ ! -z ${service} ]]
   then
    ${DOCKER_COMPOSE} ps ${service}
   else
    docker ps --filter "network=${COMPOSE_NETWORK_NAME}"
   fi

}

function clean {
    # Cleans all images created for this package.
    IMAGES="$(docker images -q --filter="label=polaris.build.source_repo=${REPO_NAME}")"
    if [ ! -z "${IMAGES}" ]; then down; docker rmi -f ${IMAGES}; fi
}

function _exec {
    declare -a args=("$@")
    if [[ ${#args[@]} -lt 2 ]]; then error "Usage: package exec service <command>"; fi
    container_cmd="${args[@]}"
    set_python_path
    message "Executing ${DOCKER_COMPOSE} exec ${container_cmd}"
    ${DOCKER_COMPOSE} exec ${container_cmd}
}

function compose  {
    compose_cmd="$@"
    set_python_path
    message "Executing ${DOCKER_COMPOSE} ${compose_cmd}"
    ${DOCKER_COMPOSE} ${compose_cmd}
}


function set_registry_relative_paths {

    if [[ ! -z "${DOCKER_REGISTRY}" ]]
    then
        case ${DOCKER_REGISTRY} in
            registry.gitlab.com)
                    REGISTRY_PACKAGE_ROOT="${DOCKER_REGISTRY}/polaris-common/polaris-build"
                    REGISTRY_POLARIS_BUILD_PATH="${REGISTRY_PACKAGE_ROOT}/polaris-build/"
                    REGISTRY_PACKAGE_PATH="${REGISTRY_PACKAGE_ROOT}/${REPO_NAME}/"
                ;;
            *)
                ;;
        esac
    fi
}

function push_image {
    image=$1
    if [[ ! -z "${image}" ]]
    then
        target_image="${REGISTRY_PACKAGE_PATH}${image}"
        message "tagging ${image} as ${target_image}"
        docker tag ${image} ${target_image}
        message "pushing ${target_image}"
        docker push ${target_image}
    else
        error "push operation was given an empty image argument"
        exit 1
    fi

}

function push {
    if [[ ! -z "${DOCKER_REGISTRY}" ]]
    then
        service=$1
        if [[ "${DOCKER_REGISTRY_TRUSTED_ENVIRONMENT}" != "true" ]]
        then
            if [[ -z "${DOCKER_REGISTRY_USERNAME}" ]]; then error "DOCKER_REGISTRY_USERNAME must be provided in the environment"; fi
            if [[ -z "${DOCKER_REGISTRY_SECRET}" ]]; then error "DOCKER_REGISTRY_SECRET must be provided in the environment"; fi

            message "logging in to docker registry at ${DOCKER_REGISTRY}"
            docker login ${DOCKER_REGISTRY} --username ${DOCKER_REGISTRY_USERNAME} --password ${DOCKER_REGISTRY_SECRET}
        fi
        if [[ -z "${service}" ]]
        then
            push_image ${PACKAGE_BASE_IMAGE}
            push_image ${PACKAGE_IMAGE}
            if [[ ! -z "${PACKAGE_DEPLOYABLE_IMAGE}" && "${PACKAGE_IMAGE}" != "${PACKAGE_DEPLOYABLE_IMAGE}" ]]
            then
                push_image ${PACKAGE_DEPLOYABLE_IMAGE}
            fi
        else
                push_image ${service}
        fi
     else
        error "DOCKER_REGISTRY environment variable must be set for push operations."
     fi
}


function list_submodules {
    if [[ -f .gitmodules ]]
    then
        echo "$(grep path .gitmodules | sed 's/.*= //')"
    fi
}

function list_initialized_submodules {
    echo $(git submodule foreach --quiet 'echo $name')
}

function exec_command_in_directory {
    declare -a args=("$@")
    target_directory=$1
    COMMAND_ARGS="${args[@]:1}"

    message "Executing command ${COMMAND_ARGS} in submodule ${target_directory} with network=${PROJECT_NETWORK}"
    pushd . &>/dev/null
    cd ${target_directory}
    if [[ -z "${PROJECT_NETWORK}" ]]
    then
        NETWORK="${REPO_NAME}"
    else
        NETWORK="${PROJECT_NETWORK}"
    fi
    PROJECT_NETWORK=${NETWORK} ${target_directory}/package ${COMMAND_ARGS}

    popd &>/dev/null
    external_command_processed=1

}



function exec_external_command {
    declare -a args=("$@")
    target_project=$1
    COMMAND_ARGS="${args[@]:1}"

    if [[ -d ./${target_project} ]]
    then

        target_directory="${BASEDIR}/${target_project}"
    fi


    if [[ -z ${target_directory} ]]
    then
        for dir in $(ls -d ${BASEDIR}/../*/); do
            if [[ "${dir}" == "${BASEDIR}/../${target_project}/" ]]
            then
              target_directory="${BASEDIR}/../${target_project}"
            fi
        done
    fi

    if [[ -z ${target_directory} ]]
    then
        for dir in $(ls -d ${BASEDIR}/../../*/); do
            if [[ "${dir}" == "${BASEDIR}/../../${target_project}/" ]]
            then
              target_directory="${BASEDIR}/../../${target_project}"
            fi
        done
    fi

    if [[ ! -z ${target_directory} ]]
    then
        exec_command_in_directory ${target_directory} ${COMMAND_ARGS}
    else
        error "Could not find external package ${target_project}"
    fi
}

function pull_image {
    image="$1"
    docker pull ${REGISTRY_PACKAGE_PATH}${image}
    docker tag ${REGISTRY_PACKAGE_PATH}${image} ${image}
}

function pull {
    if [[ ! -z "${DOCKER_REGISTRY}" ]]
    then
        service=$1
        if [[ "${DOCKER_REGISTRY_TRUSTED_ENVIRONMENT}" != "true" ]]
        then
            if [[ -z "${DOCKER_REGISTRY_USERNAME}" ]]; then error "DOCKER_REGISTRY_USERNAME must be provided in the environment"; fi
            if [[ -z "${DOCKER_REGISTRY_SECRET}" ]]; then error "DOCKER_REGISTRY_SECRET must be provided in the environment"; fi

            message "logging in to docker registry at ${DOCKER_REGISTRY}"
            docker login ${DOCKER_REGISTRY} --username ${DOCKER_REGISTRY_USERNAME} --password ${DOCKER_REGISTRY_SECRET}
        fi

        if [[ -z "${service}" ]]
        then
            pull_image ${PACKAGE_BASE_IMAGE}
            pull_image ${PACKAGE_IMAGE}
            if [[ "${PACKAGE_IMAGE}" != "${PACKAGE_DEPLOYABLE_IMAGE}" ]]
            then
                pull_image ${PACKAGE_DEPLOYABLE_IMAGE}
            fi
        else
                pull_image ${service}
        fi
     else
        error "DOCKER_REGISTRY environment variable must be set for push operations."
     fi

}

function tag_image {
    source=$1
    target=$2
    message "Tagging image ${source} as ${target}"
    docker tag ${source} ${target}
}

function strip_tag {
    image=$1
    echo "$(echo ${image} | sed 's/:.*$//')"

}





function label_image {
    image=$1
    message "Labelling ${image}"
    docker build \
           --label polaris.build.source_repo=${REPO_NAME}\
           --label polaris.build.git-sha=${GIT_SHA} \
           --label polaris.build.git-ref=${GIT_REF} \
           --label polaris.build.git-sha-short=${GIT_SHA_SHORT} \
           --label polaris.build.package-dependencies="$(build_package_dependencies)" \
           -f polaris-build/docker/polaris-image-label/Dockerfile \
           -t ${image}  \
           -t ${image}:${GIT_SHA_SHORT} \
           --build-arg IMAGE=${image} \
           --build-arg GIT_SHA=${GIT_SHA} \
           --build-arg GIT_SHA_SHORT=${GIT_SHA_SHORT} \
           polaris-build/docker/polaris-image-label

}


function cli {
    declare -a args=("$@")
    command=$1
    COMMAND_ARGS="${args[@]:1}"
    if [[  -d ${BASEDIR}/cli ]]
    then
       if [[ -f ${BASEDIR}/cli/${command} ]]
       then

            ${BASEDIR}/cli/${command} ${COMMAND_ARGS}
       else
            message "Command failed: ${BASEDIR}/cli/${command} ${COMMAND_ARGS}"
            error "Package does not have a cli function ${command} defined"
       fi
    else
        error "Package has no cli functions defined."
    fi
}




if [[ $# -ge 2 ]]
then
    declare -a args=("$@")
    declare -x BASEDIR=$1
    COMMAND=$2
    COMMAND_ARGS="${args[@]:2}"

    export PATH="${BASEDIR}:${PATH}"
    export GIT_SHA="$(git rev-parse HEAD)"
    export GIT_SHA_SHORT="$(git rev-parse --short HEAD)"
    export GIT_REF="$(git rev-parse --abbrev-ref HEAD)"


    # set default environment variables
    declare -x REPO_NAME="${BASEDIR##*/}"
    declare -x POLARIS_BUILD_DIR=""
    if [[ ${REPO_NAME} == "polaris-build" ]]; then POLARIS_BUILD_DIR="${BASEDIR}"; else POLARIS_BUILD_DIR="${BASEDIR}/polaris-build"; fi




    # Setup docker-compose file extensions scheme.
    declare COMPOSEFILE="${BASEDIR}/docker-compose.yml"
    declare PACKAGE_COMPOSEFILE="${POLARIS_BUILD_DIR}/docker/compose/docker-compose.package.yml"
    declare COMPOSE_PROJECT_NAME="${REPO_NAME}"
    if [[ ! -z "${PROJECT_NETWORK}" ]]
    then
        COMPOSE_PROJECT_NAME="${PROJECT_NETWORK}"
    fi

    declare DOCKER_COMPOSE="docker-compose -f ${COMPOSEFILE} -f ${PACKAGE_COMPOSEFILE} -p ${COMPOSE_PROJECT_NAME} "
    declare COMPOSE_NETWORK_NAME="$(echo ${COMPOSE_PROJECT_NAME} | sed s/-//g)_default"

    # Setup variables that will be used in the project docker-compose file
    declare -x PACKAGE_BASE_IMAGE="${REPO_NAME}-base"
    declare -x PACKAGE_IMAGE="${REPO_NAME}"
    declare -x PACKAGE_UWSGI_IMAGE="${PACKAGE_IMAGE}-uwsgi"
    declare -x PACKAGE_DEPLOYABLE_IMAGE=""
    declare -x PACKAGE_DEPLOYABLE_BUILD_TARGETS=""
    declare -x PACKAGE_DEPLOYABLE_IMAGE_REPOSITORY="polaris/${REPO_NAME}"
    declare -x PACKAGE_DEPLOYABLE_ARTIFACT_NAME="${REPO_NAME}"


    declare -x PYTHONPATH="${PYTHONPATH}"




    # Set up registry relative variables for push and pull operations for build artifacts
    declare -x DOCKER_REGISTRY="${DOCKER_REGISTRY}"
    declare DOCKER_REGISTRY_USERNAME="${DOCKER_REGISTRY_USERNAME}"
    declare DOCKER_REGISTRY_SECRET="${DOCKER_REGISTRY_SECRET}"
    declare -x REGISTRY_POLARIS_BUILD_PATH=""
    declare -x REGISTRY_PACKAGE_PATH=""
    if [[ ! -z "${DOCKER_REGISTRY}" ]]
    then
       set_registry_relative_paths
    fi


    # Setup variables for depolyment registry. Images are pulled from the build registry and
    # pushed to the deployment registry.
    declare -x DOCKER_DEPLOYMENT_REGISTRY="${DOCKER_DEPLOYMENT_REGISTRY}"
    if [[ -z "${DOCKER_DEPLOYMENT_REGISTRY}" ]]
    then
        DOCKER_DEPLOYMENT_REGISTRY="369272391409.dkr.ecr.us-east-1.amazonaws.com"
    fi


     # Initialize specific environment variables that are required by the build and release process.
    declare -x GITLAB_GROUP=""
    declare -x BUILDER_IMAGE=""
    declare -x RELEASE_IMAGE=""
    declare -x ALL_REQUIREMENTS_FILTER=""
    declare -x PACKAGE_PATH=""


    # load env configuration for the project if present
    if [[ -f ${BASEDIR}/.package-env ]]; then source ${BASEDIR}/.package-env; fi
    if [[ ${REPO_NAME} != 'polaris-build' ]]
    then
       if [[ -z "${BUILDER_IMAGE}" || -z "${RELEASE_IMAGE}" ]]; then error ".package-env file must define values for BUILDER_IMAGE and RELEASE_IMAGE"; fi
    fi
    if [[ -z "${PACKAGE_DEPLOYABLE_TASK_DEFINITION}" ]]
    then
        declare -x PACKAGE_DEPLOYABLE_TASK_DEFINITION="${PACKAGE_DEPLOYABLE_ARTIFACT_NAME}"
    fi
    if [[ -z "${PACKAGE_DEPLOYABLE_SERVICE}" ]]
    then
        declare -x PACKAGE_DEPLOYABLE_SERVICE="${PACKAGE_DEPLOYABLE_ARTIFACT_NAME}"
    fi




    case ${COMMAND} in
        build)
            build ${COMMAND_ARGS}
            ;;
        update)
            update ${COMMAND_ARGS}
            ;;
        run-test-setup)
            test_setup
            ;;
        run-test-teardown)
            test_teardown
            ;;
        test)
            # Runs tests against an existing library image. Does not rebuild image.
            # Intended for TDD mode development.
            test_package_base ${COMMAND_ARGS}
            ;;
        test-package)
            # Build the package image and runs tests and shuts everything down
            test_package
            ;;
        run-tests)
            # Only runs the tests. Assumes depenedent services such as databases etc are already up.
            # useful in dev mode.
            run_tests ${COMMAND_ARGS}
            ;;
        images)
            images ${COMMAND_ARGS}
            ;;
        installed)
            installed ${COMMAND_ARGS}
            ;;
        down)
            # Shuts down any running containers for this package
            down
            ;;
        up)
            up $3
            ;;
        run)
            if [ -z $3 ]; then error "Usage package run <service-name> [<args>]"; exit 1; fi
            run ${COMMAND_ARGS}
            ;;
         logs)
            if [ -z $3 ]; then error "Usage: package logs <service-name>"; exit 1; fi
            logs $3
            ;;
         ps)
            ps $3
            ;;
         shell)
            if [ -z $3 ]; then error "Usage: package shell <service>"; exit 1; fi
            run $3 bash
            ;;
         compose)
            if [ -z $3 ]; then error "Usage: package compose <docker-compose-command>"; exit 1; fi
            compose ${COMMAND_ARGS}
            ;;
         clean)
            clean
            ;;
         push)

            push $3
            ;;
         pull)
            pull $3
            ;;
         deploy-image)
            deploy_image ${COMMAND_ARGS}
            ;;
         rollback-deploy)
            rollback_deploy ${COMMAND_ARGS}
            ;;
         label-image)
            label_image ${COMMAND_ARGS}
            ;;
         cli)
            cli ${COMMAND_ARGS}
            ;;
         submodule_update)
            submodule_update ${COMMAND_ARGS}
            ;;
         list_submodules)
            list_submodules
            ;;
         changes)
            changes
            ;;
         build-package_dependencies)
            build_package_dependencies ${COMMAND_ARGS}
            ;;
         show-image-package-dependencies)
            show_image_package_dependencies ${COMMAND_ARGS} | jq
            ;;
        aws)
            source ${THIS_DIR}/aws.sh
            aws_cmd ${COMMAND_ARGS}
            ;;
        scm)
            source ${THIS_DIR}/scm.sh
            scm_cmd ${COMMAND_ARGS}
            ;;
        scaffold)
            source ${THIS_DIR}/scaffold.sh
            scaffold_cmd ${COMMAND_ARGS}
            ;;
        *)
            external_command_processed=0
            exec_external_command "${args[@]:1}"
            if [[ ${external_command_processed} -eq 0 ]]
            then
                error "Invalid command option ${COMMAND}"
            fi
            ;;
    esac

else
    message "package <package-directory> <commmand>=build|test-package|test-library|test|run|lib-run|up|build-service|list|clean"
fi



