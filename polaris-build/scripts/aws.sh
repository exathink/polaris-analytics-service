#!/usr/bin/env bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [[ -z ${AWS_ENV} ]]; then export AWS_ENV=staging; fi
source ${THIS_DIR}/../aws/${AWS_ENV}-config.sh
source ${THIS_DIR}/aws_utils.sh

function scale_service {
    instances=$1
    service=$2
    if [[ -z "${service}" ]]; then service="${PACKAGE_DEPLOYABLE_SERVICE}"; fi
    update_service_instance_count ${service} 'scal' ${instances}

}

function stop_service {
    service=$1
    if [[ -z "${service}" ]]; then service="${PACKAGE_DEPLOYABLE_SERVICE}"; fi
    instances=0
    update_service_instance_count ${service} 'stopp' ${instances}

}


function update_service_instance_count {
    service=$1
    mode=$2
    desired_count=$3
    message "${mode}ing service ${service} ..."
    current_state="$(echo $(aws_exec ecs update-service --cluster $(get_cluster ${service}) --service ${service}   --desired-count ${desired_count}) | jq '.service | {status, desiredCount, pendingCount, runningCount}')"
    desired_count=$(echo ${current_state} | jq '.desiredCount')
    running_count=$(echo ${current_state} | jq '.runningCount')
    pending_count=$(echo ${current_state} | jq '.pendingCount')
    message "running count: ${running_count} desired count: ${desired_count}"
    wait_for_service_to_stabilize "${service}"
    if [[ $? -eq 0 ]]; then message "Service ${service} ${mode}ed"; fi
}

function restart_service {
    service=$1
    instances=$2
    stop_service $1
    start_service $1 $2
}

function describe_service {
    options=$1
    if [[ -z "${options}" ]]; then options=all; fi
    service_name=$2
    if [[ -z "${service_name}" ]]; then service_name="${PACKAGE_DEPLOYABLE_SERVICE}"; fi

    aws_response="$(aws_exec ecs describe-services --cluster $(get_cluster ${service_name}) --services ${service_name})"
    echo ${aws_response} | jq_filter aws_describe_services.jq --arg show ${options}

}



function describe_tasks {

    options=$1
    if [[ -z "${options}" ]]; then options=all; fi

    service_name=$2
    if [[ -z "${service_name}" ]]; then service_name="${PACKAGE_DEPLOYABLE_SERVICE}"; fi

    task_list="$(aws_exec ecs list-tasks --cluster $(get_cluster ${service_name}) --service-name ${service_name} | jq -r '.taskArns[]' )"

    tasks="$(aws_exec ecs describe-tasks --cluster $(get_cluster ${service_name}) --tasks ${task_list})"
    echo ${tasks} | jq_filter aws_describe_tasks.jq --arg show ${options}
}

function show_logs {
    # Shows non-ping entries in the log. If no time argument is provided then it returns the last minute of logs
    # if you want to go back longer pass a time argument in minutes

    service_name=$1
    if [[ -z "${service_name}" ]]; then service_name="${PACKAGE_DEPLOYABLE_SERVICE}"; fi

    container_name=$2
    if [[ -z "${container_name}" ]]; then container_name="${service_name}"; fi

    time=$3
    if [[ -z "${time}" ]]; then time=60; else time=$((${time}*60)); fi

    now=$(date +%s)
    start_time=$((${now} - ${time}))
    START_FILTER="--start-time $((${start_time}*1000))"

    log_streams="$(describe_tasks stableTaskIds ${service_name}  | jq -r "map(\"${service_name}/${container_name}/\" + .)[]")"
    echo "$(aws_exec logs filter-log-events --log-group-name $(get_log_group ${service_name}) --log-stream-names ${log_streams} ${START_FILTER} --filter-pattern '- ping' | jq -r  '.events | map(.message)[]')"
    message "Displayed non-ping log entries for the last $((${time}/60)) minutes"
}

function show_pings {
    # Shows non-ping entries in the log. If no time argument is provided then it returns the last minute of logs
    # if you want to go back longer pass a time argument in seconds.
    service_name=$1
    if [[ -z "${service_name}" ]]; then service_name="${PACKAGE_DEPLOYABLE_SERVICE}"; fi

    time=$2
    if [[ -z "${time}" ]]; then time=60; else time=$((${time}*60)); fi



    now=$(date +%s)
    start_time=$((${now} - ${time}))
    START_FILTER="--start-time $((${start_time}*1000))"
    log_streams="$(describe_tasks stableTaskIds ${service_name}  | jq -r "map(\"${service_name}/${service_name}/\" + .)[]")"
    echo "$(aws_exec logs filter-log-events --log-group-name $(get_log_group ${service_name}) --log-stream-names ${log_streams} ${START_FILTER} --filter-pattern 'ping'  | jq -r  '.events | map(.message)[]')"
    message "Displayed ping log entries for the last $((${time}/60)) minutes"
}




function deploy_image {
    commit_tag=$1
    if [[ -z "${commit_tag}" ]]
    then
        source_image="${PACKAGE_DEPLOYABLE_IMAGE}:latest"
    else
        source_image="${PACKAGE_DEPLOYABLE_IMAGE}:${commit_tag}"
    fi

    target_repository="${PACKAGE_DEPLOYABLE_IMAGE_REPOSITORY}"
    target_image="${DOCKER_DEPLOYMENT_REGISTRY}/${target_repository}"
    declare -a args=("$@")
    additional_tags="${args[@]:1}"

    source_image_id="$(docker images -q --filter="reference=${source_image}")"
    if [[ ! -z "${source_image_id}" ]]
    then
        image_details="$(docker inspect ${source_image_id})"
        image_git_sha="$(echo ${image_details} | jq -r '.[0].Config.Labels["polaris.build.git-sha-short"]')"
        if [[ -z "${commit_tag}" || "${image_git_sha}" == "${commit_tag}" ]]
        then
            target_tags="${AWS_ENV} ${image_git_sha} ${additional_tags}"

            message2 "Preparing to deploy source image: ${source_image} (${source_image_id})"
            echo "${image_details}" | jq '.[0] | {id: .Id, tags: .RepoTags, labels: .Config.Labels }'
            message2 "Images will be pushed to remote repository: ${target_image}"
            message2 "Tags to be pushed: ${target_tags}"
            confirm_y_n "Continue? "
            if [[ ${REPLY} == 'y' ]]
            then

                echo "image git sha: ${image_git_sha}"
                message "Logging in to ${DOCKER_DEPLOYMENT_REGISTRY}"
                $(aws ecr get-login --no-include-email --region us-east-1)
                for tag in ${target_tags}; do
                    docker tag ${source_image} ${target_image}:${tag}
                    message "Pushing tag ${tag}"
                    docker push ${target_image}:${tag}
                done

                message "Image pushed to remote repository ${target_image}"
                show_repository "${target_repository}" "${image_git_sha}"
                # tagging commit with image details
                tag_name="deployed-to-${AWS_ENV}-$(date +%Y-%m-%d-%H-%M-%S)"
                tag_details="Image ${target_image}:${image_git_sha}"
                git tag -f -a "${tag_name}" -m "${tag_details}"
                message "Tagged commit ${image_git_sha} as ${tag_name}"
            else
                exit 0
            fi
        else
            error "Aborting deployment: git-sha label of image ${source_image} is ${image_git_sha}
                   which does not match the commit ${commit_tag} requested for deployment"
            exit 1
        fi
     else
       error "Could not find image ${source_image}"
       exit 1
     fi

}

function deploy_task_definition {

    task_definition=$1
    container_name=$2
    source_image=$3

    if [[ -z "${task_definition}" ]]; then task_definition="${PACKAGE_DEPLOYABLE_TASK_DEFINITION}"; fi
    if [[ -z "${container_name}" ]]; then container_name="${PACKAGE_DEPLOYABLE_TASK_DEFINITION}"; fi
    if [[ -z "${source_image}" ]]; then source_image="${PACKAGE_DEPLOYABLE_IMAGE}:latest"; fi

    existing_task_definition="$(task_definition_by_image "$(get_deployment_image_sha "${source_image}")" ${task_definition})"
    if [[ ! -z "${existing_task_definition}" ]]
    then
        warning "A task definition with the specified source image is already deployed."
        echo "${existing_task_definition}" | jq
        confirm_y_n "Continue with deployment? "
        if [[ ${REPLY} != 'y' ]]; then message "Deployment cancelled."; return 0; fi
    fi
    source_image_id="$(get_image_id ${source_image})"
    if [[ ! -z "${source_image_id}" ]]
    then
        image_git_sha="$(get_image_sha ${source_image_id})"
        if [[ ! -z "${image_git_sha}" ]]
        then
            target_repository="${PACKAGE_DEPLOYABLE_IMAGE_REPOSITORY}"
            echo "${target_repository}" "${image_git_sha}"

            if [[ ! -z "$(show_repository_image_with_tag "${target_repository}" "${image_git_sha}")" ]]
            then
                update_container_json="$(update_task_definition_set_container_image_json "${task_definition}" "${container_name}" "${target_repository}" "${image_git_sha}")"
                if [[ ! -z "${update_container_json}" ]]
                then
                    message2 "Updating container definition ${container_name} for task definition ${task_definition}"
                    message2 "New definition"
                    echo "${update_container_json}" | jq
                    message2 "Image for container ${container_name} will be updated to: $(echo "${update_container_json}" | jq ".containerDefinitions | map(select(.name == \"${container_name}\"))[0].image")"
                    confirm_y_n "Update with above definitions? "
                    if [[ ${REPLY} == 'y' ]]
                    then
                        echo "$(update_task_definition ${task_definition} ${update_container_json})" | jq
                    else
                        return 1
                    fi
                else
                error "Fatal error: Could not build update container command."
                return 1
                fi
            else
                error "Image ${source_image} has not been deployed to remote repository"
                return 1
            fi
        else
            error "Image ${source_image} is not labeled with a git sha"
            return 1
        fi
    else
        error "Image ${source_image} was not found"
        return 1
    fi

}

function deploy_service {

    service="$1"
    if [[ -z "${service}" ]]; then service="${PACKAGE_DEPLOYABLE_SERVICE}"; fi


    task_definition_family="$2"
    if [[ -z "${task_definition_family}" ]]; then task_definition_family="${service}"; fi

    container_name="$3"
    if [[ -z "${container_name}" ]]; then container_name="${task_definition_family}"; fi

    source_image="$4"
    if [[ -z "${source_image}" ]]; then source_image="${PACKAGE_DEPLOYABLE_IMAGE}:latest"; fi

    message "Deploying service: ${service}"
    message "Target task definition: ${task_definition_family}"
    message "Target container name: ${container_name}"


    deployed_image="$(get_deployment_image_sha "${source_image}")"
    message "Searching for target task definition using image ${deployed_image}.."
    target_task_definition="$(show_task_definitions by-image ${deployed_image} ${task_definition_family} ${container_name})"

    if [[ ! -z "${target_task_definition}" ]]
    then
        target_task_definition_arn="$(echo "${target_task_definition}" | jq '.taskDefinitionArn')"
        message "Found task definition:"
        echo "${target_task_definition_arn}" | jq
        current_service_task_definition_arn="$(describe_service task-definition ${service})"
        if [[ "${target_task_definition_arn}" != "${current_service_task_definition_arn}" ]]
        then
            message "Service will be updated to use the following task definition"
            echo ${target_task_definition} | jq
            confirm_y_n
            if [[ ${REPLY} == 'y' ]]
            then
                task_definition_arn="$(echo "${target_task_definition}"  | jq -r '.taskDefinitionArn')"
                aws_response=$(aws_exec ecs update-service --cluster "$(get_cluster ${service})" --service ${service} --task-definition "${task_definition_arn}")
                if [[ $? -eq 0 ]]
                then
                    message "Service update was successful."
                    message "Current service status:"
                    echo "${aws_response}" \
                                 | jq '.service
                                        | {
                                            serviceArn,
                                            status,
                                            taskDefinition,
                                            runningCount,
                                            pendingCount,
                                            desiredCount,
                                            deployments: .deployments | map( {
                                                status,
                                                taskDefinition,
                                                desiredCount,
                                                runningCount,
                                                createdAt: .createdAt | todate,
                                                updatedAt: .updatedAt | todate,
                                            })
                                          }'
                    wait_for_service_to_stabilize ${service}
                 else
                    error "Deployment failed: AWS API call failed  with error code $?"
                    return $?
                 fi
            fi
        else
            warning "Current task definition version for deployed service is the same as the target task definition"
            warning "Deployed service:"
            describe_service summary ${service} | jq
            message2 "No deployment will be done."
            return 0
        fi
    else
        error "No applicable task definition was found for image ${deployed_image}. Service deployment aborted."
        return 1
    fi
}

function deploy_commit {
    commit=$1
    if [[ -z "${commit}" ]]; then commit="${GIT_SHA_SHORT}"; fi
    source_image="${PACKAGE_DEPLOYABLE_IMAGE}:${commit}"
    message2 "Deploying image: ${source_image}"
    deploy_image "${commit}"
    latest_task_definition="$(task_definition_latest)"
    if [[ ! -z  "${latest_task_definition}" ]]
    then
        message "Updating task definition.."
        message "Latest task definition is ${latest_task_definition}"
        deploy_task_definition "${source_image}"
    fi
    service="$(find_service)"
    if [[ ! -z "${service}" ]]
    then
        deploy_service "${service}" "${source_image}"
    fi

}

function show_deployment_events {
    service=$1
    if [[ -z "${service}" ]]; then service="${PACKAGE_DEPLOYABLE_SERVICE}"; fi
    wait_for_service_to_stabilize "${service}" false
}



function get_configs {
    service=$1
    if [[ -z "${service}" ]]; then service="${PACKAGE_DEPLOYABLE_SERVICE}"; fi
    echo "$(get_s3  "${S3_CONFIG_BUCKET}" "$(get_configs_object_key "${service}")" )"

}
function update_configs {

   service=$1
   if [[ -z "${service}" ]]; then error "Usage aws config update <service> name=value [name=value]"; exit 1; fi

   declare -a args=("$@")
   configs_to_add="${args[@]:1}"


   configs="$(get_configs "${service}")"
   for assignment in ${configs_to_add}; do
    entry="$(echo "${assignment}" | jq -Rr 'capture("(?<lhs>[a-zA-Z0-9_-]*)=(?<rhs>.*)") | {(.lhs): .rhs}')"
    configs="$(echo "${configs}" | jq ". + ${entry}")"
   done
   message2 "Update configs for ${service} to:"
   echo "${configs}" | jq
   confirm_y_n
   if [[ ${REPLY} == 'y' ]]
   then
    key="$(get_configs_object_key "${service}")"
    aws_response="$(put_s3  "${S3_CONFIG_BUCKET}" "${key}" "${configs}" )"
    if [[ "$(echo "${aws_response}" | jq 'has("ETag")')" == "true" ]]
    then
        message "configs updated successfully."

    else
        error "configs were not updated: ${aws_response}"
        return 1
    fi
   fi
}

function clear_configs {
    service=$1
    if [[ -z  ${service} ]]; then service="${PACKAGE_DEPLOYABLE_SERVICE}"; fi
    aws_response="$(put_s3 "${S3_CONFIG_BUCKET}" "$(get_configs_object_key "${service}")" "{}")"
    if [[ "$(echo "${aws_response}" | jq 'has("ETag")')" == "true" ]]
    then
        message "configs cleared successfully."

    else
        error "configs were not cleared: ${aws_response}"
        return 1
    fi

}

function put_config {
    service=$1
    if [[ -z  "${service}" ]]; then service="${PACKAGE_DEPLOYABLE_SERVICE}"; fi

    config_file="$2"
    if [[ ! -f ${config_file} ]]; then error "Config file ${config_file} was not found";  return 1; fi

    aws_response="$(put_s3 "${S3_CONFIG_BUCKET}" "$(get_configs_object_key "${service}")" "$(cat ${config_file})")"
    if [[ "$(echo "${aws_response}" | jq 'has("ETag")')" == "true" ]]
    then
        message "configs put successfully."

    else
        error "configs were not cleared: ${aws_response}"
        return 1
    fi


}

function config {
    operation=$1
    declare -a args=($@)
    command_args="${args[@]:1}"

    case ${operation} in
        add | update )
            update_configs ${command_args}
            ;;
        show)
            get_configs ${command_args}
            ;;
        clear)
            clear_configs ${command_args}
            ;;
        put)
            put_config ${command_args}
            ;;
        *)
            error "Usage: aws config <add | update> ..."
            exit 1
            ;;
    esac
}



function ssh_key_cmd {
    declare -a args=("$@")
    command=$1
    COMMAND_ARGS="${args[@]:1}"

    case ${command} in
        create)
            create_ssh_key ${COMMAND_ARGS}
            ;;
        fetch-public)
            fetch_ssh_public_key ${COMMAND_ARGS}
            ;;
        *)
            error "Unknown ssh_keys command ${command}"; exit 1;
            ;;
    esac
}

function aws_cmd {
    declare -a args=("$@")
    command=$1
    COMMAND_ARGS="${args[@]:1}"

    case ${command} in
        describe-service)
            describe_service ${COMMAND_ARGS} | jq
            ;;
        describe-tasks)
            describe_tasks ${COMMAND_ARGS} | jq
            ;;
        create-log-group)
            create_log_group ${COMMAND_ARGS} | jq
            ;;
        show-logs)
            stream_logs ${COMMAND_ARGS}
            ;;
        show-pings)
            show_pings ${COMMAND_ARGS}
            ;;
        list-services)
            list_services ${COMMAND_ARGS} | jq
            ;;
        find-service)
            find_service ${COMMAND_ARGS} | jq
            ;;
        scale-service)
            scale_service ${COMMAND_ARGS}
            ;;
        stop-service)
            stop_service ${COMMAND_ARGS}
            ;;
        restart-service)
            restart_service ${COMMAND_ARGS}
            ;;
        run-task)
            run_ecs_task ${COMMAND_ARGS}
            ;;
        deploy-image)
            deploy_image ${COMMAND_ARGS}
            ;;
        create-repository)
            create_repository ${COMMAND_ARGS} | jq
            ;;
        show-repository)
            show_repository ${COMMAND_ARGS}
            ;;
        show-task-definitions)
            show_task_definitions ${COMMAND_ARGS} | jq
            ;;
        deploy-task-definition)
            deploy_task_definition ${COMMAND_ARGS}
            ;;
        deploy-service)
            deploy_service ${COMMAND_ARGS}
            ;;
        show-deployment-events)
            show_deployment_events ${COMMAND_ARGS}
            ;;
        show-fatal)
            show_fatal_events ${COMMAND_ARGS}
            ;;

        deploy-commit)
            deploy_commit ${COMMAND_ARGS}
            ;;
        config)
            config ${COMMAND_ARGS}
            ;;
        keys)
            ssh_key_cmd ${COMMAND_ARGS}
            ;;
        get-s3)
            get_s3 ${COMMAND_ARGS}
            ;;
        put-s3)
            put_s3 ${COMMAND_ARGS}
            ;;
        *)
            cli_aws_hook_script="${BASEDIR}/cli/${command}.sh"
            if [[ -f "${cli_aws_hook_script}" ]]
            then
                ${cli_aws_hook_script} ${COMMAND_ARGS}
            else
                error "Unrecognized aws command ${command}"
                exit 1
            fi
   esac
}