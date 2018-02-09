#!/usr/bin/env bash

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${THIS_DIR}/utils.sh"

export AWS_CMD="aws --output json"

function aws_exec {
    ${AWS_CMD} "$@"

}

# --- Utilities for running ecs tasks -----------

function extract_task_id_from_tasks {
    json=$1
    echo "$(echo ${json} | jq '.tasks | map(.taskArn | capture("task/(?<taskId>.*)")) | map(.taskId)')"
}
function fill_command_array_from_args {
    tokens=("$@")
    command="\"${tokens[0]}\""
    for token in "${tokens[@]:1}"; do
        command="${command}, \"${token}\""
    done
    echo "${command}"

}

function stream_logs {

    service_name=$1
    if [[ -z "${service_name}" ]]; then service_name="${PACKAGE_DEPLOYABLE_SERVICE}"; fi

    container_name=$2
    if [[ -z "${container_name}" ]]; then container_name="${service_name}"; fi

    task_id=$3
    if [[ -z "${task_id}" ]]
    then
        log_stream="$(describe_tasks stableTaskIds ${service_name}  | jq -r "\"${service_name}/${container_name}/\" + .[0]")"
    else
        log_stream="${service_name}/${container_name}/${task_id}"
    fi

    message "Displaying logs for log stream: ${log_stream}"
    aws_response="$(echo "$(aws_exec logs get-log-events --log-group-name $(get_log_group ${service_name}) \
                                            --log-stream-name ${log_stream} \
                                            --start-from-head \
                                            )")"
    echo "$(echo ${aws_response} | jq -r '.events | map(.message)[]')"
    next_token="$(echo ${aws_response} | jq -r '.nextForwardToken')"

    while [[ ! -z "${next_token}" ]]
    do
        aws_response="$(echo "$(aws_exec logs get-log-events --log-group-name $(get_log_group ${service_name}) \
                                            --log-stream-name ${log_stream} \
                                            --next-token ${next_token}\
                                            )")"
        messages="$(echo ${aws_response} | jq -r '.events | map(.message)[]')"
        if [[ ! -z "${messages}" ]]
        then
            for message in "${messages}"; do
                echo "${message}"

            done
        else
            sleep 10
        fi
        next_token="$(echo ${aws_response} | jq -r '.nextForwardToken')"
    done


}

function get_task_failures {
    tasks=$@
    task_failures="$(aws_exec ecs describe-tasks --cluster ${CLUSTER} --tasks ${tasks} \
                    | jq '.tasks[0].containers
                    | map(select(.exitCode != 0 ))
                    | map({container: .name, lastStatus, exitCode, containerArn, taskArn})')"
    if [[ $(echo "${task_failures}" | jq 'length') -gt 0 ]]
    then
        echo "${task_failures}"
    fi
}

function show_task_run {
    aws_response=$1


    if [[ ! -z "${VERBOSE}" ]]
    then
        message "ECS Task Created..."
        message "Task details:"
        echo "${aws_response}" | jq
    else
        message "ECS Task Created: $(echo "${aws_response}" | jq '.tasks[0] | {taskDefinitionArn, taskArn, lastStatus,  createdAt: (.createdAt | todate)}')"
    fi
    tasks="$(echo ${aws_response} | jq -r '.tasks| map(.taskArn)[]')"
    log_streams="$(echo ${aws_response} | jq '.tasks | map(.taskArn | capture("task/(?<taskId>.*)")) | map(.taskId)' | jq -r "map(\"${container_name}/${container_name}/\" + .)[]")"

    wait_for_tasks_to_start ${tasks}
    tasks_finished="false"
    last_timestamp=0
    while [[ "${tasks_finished}" == "false" ]]
    do
        task_state="$(aws_exec ecs describe-tasks --cluster ${CLUSTER} --tasks ${tasks})"
        tasks_finished="$(echo "${task_state}" | jq '.tasks | map(.lastStatus == "STOPPED") | all')"

        log_output=$(aws_exec logs filter-log-events --log-group-name=${taskdef} --log-stream-names ${log_streams} --start-time $((last_timestamp+1)) )
        # echo ${log_output} | jq
        num_events="$(echo ${log_output} | jq '.events| length')"

        if [[ ${num_events} -gt 0 ]]
        then
            last_timestamp="$(echo ${log_output} | jq '.events | map(.timestamp) | last' )"
            echo ${log_output} | jq -rC '.events | map(((.timestamp / 1000) | todate ) + " " + .message)[]'
        fi
    done
    task_failures="$(get_task_failures ${tasks})"
    if [[ ! -z "${task_failures}" ]]
    then
        error "Task did not complete successfully"
        if [[ ! -z "${VERBOSE}" ]]; then echo "${task_failures}" | jq; fi
        return $(echo "${task_failures}" | jq '.[0].exitCode')

    else
        message "Task completed successfully"
        return 0
    fi
}

function wait_for_tasks_to_start {
    tasks="$@"
    started="false"
    message "Waiting for tasks to run.."
    while [[ ${started} == "false" ]]
    do
        task_state="$(aws_exec ecs describe-tasks --cluster ${CLUSTER} --tasks ${tasks})"
        started="$(echo "${task_state}" | jq '.tasks | map(.lastStatus != "PENDING") | all')"
        sleep 2
    done
}

function run_ecs_task {
    args=("$@")
    taskdef=$1
    if [[ -z "${taskdef}" ]]; then error "Usage: aws run-task <taskdefinition> [ <container_name> ]"; exit 1; fi

    container_name=$2
    if [[ -z "${container_name}" ]]; then container_name="${taskdef}"; fi

    instance_type=$3
    if [[ -z "${instance_type}" ]]; then instance_type="*"; fi

    override_json="{
        \"containerOverrides\": [
            {
                \"name\": \"${container_name}\",
                \"command\": [ $(fill_command_array_from_args ${args[@]:3} )]
            }
        ]
    }"

    placement_constraints="[
            {
                \"type\": \"memberOf\",
                \"expression\": \"attribute:ecs.instance-type == ${instance_type}\"
            }
    ]"
    aws_response="$(aws_exec ecs run-task --cluster ${CLUSTER} --task-definition "${taskdef}" --overrides "${override_json}" --placement-constraints "${placement_constraints}")"

    if [[ $? -eq 0 ]]
    then
        show_task_run "${aws_response}"
    else
        error "Could not run ecs task"
        echo "${aws_response}" | jq
        exit 1
    fi
}

# -- ECR Repository Utilities ----
function create_repository {
    repository=$1
    if [[ -z "${repository}" ]]; then repository="${PACKAGE_DEPLOYABLE_IMAGE_REPOSITORY}"; fi

    echo "$(aws_exec ecr create-repository --repository-name "${repository}")"

}

function exec_show_repository_cmd {
    repository=$1

    next_token=$2
    next_token_opt=""
    if [[ ! -z "${next_token}" ]]; then next_token_opt=" --starting-token ${next_token} "; fi

    num_items=$3
    if [[ ! -z "${num_items}" ]]; then pagination_opt=" --page-size ${num_items} --max-items ${num_items} "; fi
    echo "$(aws_exec ecr describe-images --repository-name ${repository} ${pagination_opt} ${next_token_opt})"

}

function get_repository_image_details {
 aws_ecs_describe_images_response=$1
 echo "$(echo "${aws_ecs_describe_images_response}"  \
                        | jq '.imageDetails
                                |  map({
                                    repositoryName,
                                    imageDigest,
                                    imageTags,
                                    imageSizeMB: (.imageSizeInBytes / 1048576),
                                    imagePushedAt: .imagePushedAt | todate
                                   })'
                    )"

}

function show_repository_iterator {
   repository=$1
   callback=$2
   num_items=$3
   show_repository_next=""

   while [[ "${show_repository_next}" != "null" ]]; do
       aws_response="$(exec_show_repository_cmd $1 "${show_repository_next}" "${num_items}")"
       show_repository_image_details="$(get_repository_image_details "${aws_response}")"

        show_repository_next="$(echo ${aws_response} | jq -r '.NextToken')"
        ${callback} "${show_repository_image_details}"
   done

}

function show_repository_callback {
    echo $1 | jq
}

function show_repository {
    repository=$1
    if [[ -z "${repository}" ]]; then repository="${PACKAGE_DEPLOYABLE_IMAGE_REPOSITORY}"; fi
    tag=$2
    if [[ -z "$tag" ]]
    then
        show_repository_iterator "${repository}" 'show_repository_callback'
    else
        echo "$(show_repository_image_with_tag "${repository}" "${tag}")" | jq
    fi
}



function show_repository_image_with_tag {
   repository=$1
   tag=$2
   num_items=$3
   show_repository_next=""

   while [[ "${show_repository_next}" != "null" ]]; do
       aws_response="$(exec_show_repository_cmd $1 "${show_repository_next}" "${num_items}")"
       image_details="$(get_repository_image_details "${aws_response}")"

        tagged_image="$(echo "${image_details}" | jq "map(select(.imageTags | map( . == \"${tag}\")| any ))")"
        if [[ $(echo "${tagged_image}" | jq 'length') -gt 0 ]]
        then
            echo ${tagged_image}
            break
        else
            show_repository_next="$(echo ${aws_response} | jq -r '.NextToken')"
        fi
   done

}

# --- Utilities for automating deployments ----------------------
#

function list_services {
    aws_response="$(aws_exec ecs list-services --cluster ${CLUSTER})"
    echo "$(echo "${aws_response}" | jq '.serviceArns')"
}

function find_service {
    service=$1
    if [[ -z "${service}" ]]; then service="${PACKAGE_DEPLOYABLE_SERVICE}"; fi

    echo "$( list_services | jq "map(select(test(\"${service}\")))[]")"
}

function show_service_events {
    service=$1
    since=$2
    aws_response=$3
    if [[ -z "${since}" ]]; then since=0; fi
    if [[ -z "${aws_response}" ]]; then aws_response="$(aws_exec ecs describe-services --cluster $(get_cluster ${service}) --services ${service})"; fi

    echo "${aws_response}" | jq ".services[0].events | map(select( .createdAt > ${since})) | reverse"


}

function print_service_events {
    echo "$1" |jq -r 'map((.createdAt | todate) + ": " + .message)[]'
}

function fatal_events_in_event_stream {
    echo "$(echo "$1" | jq -r 'map(.message | select(test("unable to place a task")))[]')"
}

function show_fatal_events {
    service=$1
    aws_response="$(aws_exec ecs describe-services --cluster $(get_cluster ${service}) --services ${service})"
    events="$(echo "${aws_response}" | jq '.services[0].events')"
    fatal="$(fatal_events_in_event_stream "${events}")"
    echo "${fatal}"

}

function wait_for_service_to_stabilize {
    service=$1
    abort_on_fatal_errors=$2
    if [[ -z "${abort_on_fatal_errors}" ]]; then abort_on_fatal_errors=true; fi



    aws_response="$(aws_exec ecs describe-services --cluster $(get_cluster ${service}) --services ${service})"
    desired_count="$(echo "${aws_response}" | jq '.services[0].desiredCount')"
    running_count="$(echo "${aws_response}" | jq '.services[0].runningCount')"
    pending_count="$(echo "${aws_response}" | jq '.services[0].pendingCount')"
    deployment_count="$(echo "${aws_response}" | jq '.services[0].deployments|length')"

    # Filter the event stream to start from the point the primary deployment was last updated.
    event_window_start="$(echo "${aws_response}" | jq '.services[0].deployments|map(select(.status == "PRIMARY"))[0]| .updatedAt')"
    if [[ -z "${event_window_start}" ]]; then event_window_start=$(now); fi


    operation_failed=false
    if [[ ${desired_count} -ne ${running_count}  || ${deployment_count} -gt 1 ]]
    then
        message "Service update in progress....";
        echo "$(describe_service "update-status" ${service} )" | jq

        while [[ ${desired_count} -ne ${running_count} || ${pending_count} -gt 0  || ${deployment_count} -gt 1 ]]; do
            sleep 5
            service_events="$(show_service_events ${service}  ${event_window_start})"
            if [[ $(echo "${service_events}" | jq 'length') -gt 0 ]]
            then
                print_service_events "${service_events}"
                event_window_start=$(echo "${service_events}" | jq 'last | .createdAt')
                if [[ ! -z "$(fatal_events_in_event_stream "${service_events}")" ]]
                then
                    error "Fatal errors detected in event logs..."
                    operation_failed=true
                    if [[ ${abort_on_fatal_errors} == true ]]; then break; fi
                fi
            fi
            aws_response="$(aws_exec ecs describe-services --cluster $(get_cluster ${service}) --services ${service})"
            desired_count="$(echo "${aws_response}" | jq '.services[0].desiredCount')"
            running_count="$(echo "${aws_response}" | jq '.services[0].runningCount')"
            pending_count="$(echo "${aws_response}" | jq '.services[0].pendingCount')"
            deployment_count="$(echo "${aws_response}" | jq '.services[0].deployments|length')"

        done
        if [[  ${operation_failed} == false ]]
        then
            # final wait to see if any last events can be flushed until service reaches steady state
            sleep 60
            service_events="$(show_service_events ${service}  ${event_window_start})"
            print_service_events "${service_events}"
        fi
    else
        service_events="$(show_service_events ${service}  ${event_window_start})"
        print_service_events "${service_events}"
    fi
    echo "$(describe_service summary ${service})" | jq
    if [[ ${operation_failed} == false ]]; then message "Service update complete"; return 0; else error "Service update failed"; return 1; fi
}



function update_task_definition_set_container_image_json {
    task_definition=$1
    container_name=$2
    target_repository=$3
    target_tag=$4
    target_image="${DOCKER_DEPLOYMENT_REGISTRY}/${target_repository}:${target_tag}"
    image_labels="$(echo "$(docker inspect ${target_image} | jq '.[0].Config.Labels')")"
    git_ref="$(echo "${image_labels}" | jq '.["polaris.build.git-ref"]')"
    git_sha="$(echo "${image_labels}" | jq '.["polaris.build.git-sha"]')"
    git_sha_short="$(echo "${image_labels}" | jq '.["polaris.build.git-sha-short"]')"
    package_dependencies="$(echo "${image_labels}" | jq '.["polaris.build.package-dependencies"]')"
    source_repo="$(echo "${image_labels}" | jq '.["polaris.build.source_repo"]')"
    updated_definition="$(aws_exec ecs describe-task-definition --task-definition ${task_definition} \
                            |  jq ".taskDefinition
                                        | .containerDefinitions=(
                                                        .containerDefinitions | map(
                                                            if .name == \"${container_name}\"
                                                            then
                                                                .image=\"${target_image}\" |
                                                                .environment=(.environment | map(select(
                                                                    .name != \"environment\" and
                                                                    .name != \"config_provider\" and
                                                                    .name != \"config_context\"
                                                                ))
                                                                +
                                                                [
                                                                   { \"name\": \"environment\", \"value\": \"${AWS_ENV}\" },
                                                                   { \"name\": \"config_provider\", \"value\": \"polaris.aws.utils.config\" },
                                                                   { \"name\": \"config_context\", \"value\": \"${task_definition}\"}
                                                                 ])|
                                                                 .dockerLabels=(.dockerLabels + {
                                                                    \"polaris.build.source_repo\": ${source_repo},
                                                                    \"polaris.build.git-ref\": ${git_ref},
                                                                    \"polaris.build.git-sha\": ${git_sha},
                                                                    \"polaris.build.git-sha-short\": ${git_sha_short},
                                                                    \"polaris.build.package-dependencies\": ${package_dependencies}
                                                                 })

                                                            else . end
                                                        )
                                            )
                                         | { family, containerDefinitions, volumes, placementConstraints} + { taskRoleArn: (.taskRoleArn // \"\") }
                                         "
                         )"

    echo "${updated_definition}"
}

function update_task_definition {
    task_definition=$1
    cli_input_json=$2
    json_file="$(mktemp)"
    echo "${update_container_json}" > ${json_file}
    aws_response="$(aws_exec ecs register-task-definition --family ${task_definition} --cli-input-json file://${json_file})"
    echo ${aws_response}
    rm -f json_file
}

function exec_list_task_definitions_cmd {
    task_definition_family=$1
    next_token=$2

    next_token_opt=""
    if [[ ! -z "${next_token}" ]]; then next_token_opt=" --starting-token ${next_token} "; fi

    num_items=$3
    if [[ ! -z "${num_items}" ]]; then pagination_opt=" --page-size ${num_items} --max-items ${num_items} "; fi
    echo "$(aws_exec ecs list-task-definitions --family-prefix ${task_definition_family} --status ACTIVE --sort DESC ${pagination_opt} ${next_token_opt})"


}

function task_definition_by_image {
    target_image=$1
    if [[ -z "${target_image}" ]]; then target_image="$(get_deployment_image_sha)"; fi

    task_definition_family=$2
    if [[ -z "${task_definition_family}" ]]; then task_definition_family="${PACKAGE_DEPLOYABLE_TASK_DEFINITION}"; fi

    container_name=$3
    if [[ -z "${container_name}" ]]; then container_name="${PACKAGE_DEPLOYABLE_TASK_DEFINITION}"; fi



    list_task_definition_next=""
    found=false
    while [[ ${found} == false && "${list_task_definition_next}" != "null" ]]; do
        aws_response="$(exec_list_task_definitions_cmd "${task_definition_family}"  "${list_task_definition_next}" )"
        for arn in $(echo "${aws_response}" | jq -r '.taskDefinitionArns[]'); do
            remote_task_definition="$(aws_exec ecs describe-task-definition --task-definition "${arn}")"
            # Find the task definition by image and environment. We need to match the environment specifically
            # because configs for the containers are loaded by environment. Which means when an image is promoted to
            # a new environment, a new task definition has to be deployed - or else we will risk ending up with
            # containers using the wrong configs for the environment.
            if [[ ! -z "$(echo "${remote_task_definition}" \
                                | jq "select(.taskDefinition.containerDefinitions
                                                   | map( .name == \"${container_name}\"
                                                          and .image == \"${target_image}\"
                                                          and (.environment
                                                                    | any(
                                                                        .name == \"environment\"
                                                                        and .value == \"${AWS_ENV}\"
                                                                        )
                                                               )) | any)")" ]]
            then
                echo "$(task_definition_summary "${remote_task_definition}")"
                found=true
                break;
            fi
        done
        list_task_definition_next="$(echo ${aws_response} | jq -r '.NextToken')"
    done
}

function task_definition_versions {
    task_definition_family=$1
    if [[ -z "${task_definition_family}" ]]; then task_definition_family="${PACKAGE_DEPLOYABLE_TASK_DEFINITION}"; fi

    list_task_definition_next=""

    while [[ "${list_task_definition_next}" != "null" ]]; do
        aws_response="$(exec_list_task_definitions_cmd "${task_definition_family}"  "${list_task_definition_next}" )"
        echo "${aws_response}"
        list_task_definition_next="$(echo ${aws_response} | jq -r '.NextToken')"
    done
}

function task_definition_summary {
    remote_task_definition=$1
    echo "${remote_task_definition}" \
                    | jq '.taskDefinition | {
                            taskDefinitionArn,
                            status,
                            containerDefinitions: (.containerDefinitions | map({name, image}))
                        }'
}

function task_definition_summaries {

    local max_items=$1
    local task_definition_family=$2
    if [[ -z "${task_definition_family}" ]]; then task_definition_family="${PACKAGE_DEPLOYABLE_TASK_DEFINITION}"; fi

    local list_task_definition_next=""
    local items=0

    while [[ "${list_task_definition_next}" != "null" ]]; do
        aws_response="$(exec_list_task_definitions_cmd "${task_definition_family}"  "${list_task_definition_next}" )"
        for arn in $(echo "${aws_response}" | jq -r '.taskDefinitionArns[]'); do
            remote_task_definition="$(aws_exec ecs describe-task-definition --task-definition "${arn}")"
            echo "$(task_definition_summary "${remote_task_definition}")"

            items=$((${items}+1))
            if [[ ! -z "${max_items}" &&  ${items} -ge ${max_items} ]]; then break; fi
        done
        if [[ ! -z "${max_items}" &&  ${items} -ge ${max_items} ]]; then break; fi
        list_task_definition_next="$(echo ${aws_response} | jq -r '.NextToken')"
    done
}

function task_definition_version_details {
    revision=$1
    if [[ -z "${revision}" ]]; then error "Usage: aws show-task-definition revision <revision_number>"; exit 1; fi

    task_definition_family=$2
    if [[ -z "${task_definition_family}" ]]; then task_definition_family="${PACKAGE_DEPLOYABLE_TASK_DEFINITION}"; fi
    echo "$(aws_exec ecs describe-task-definition --task-definition "${task_definition_family}:${revision}")"
}

function task_definition_latest {
    task_definition=$1
    if [[ -z "${task_definition}" ]]; then task_definition=${PACKAGE_DEPLOYABLE_TASK_DEFINITION}; fi
    aws_response="$(exec_list_task_definitions_cmd ${task_definition})"

    echo "$(echo "${aws_response}" | jq  '.taskDefinitionArns[0]')"
}

function show_task_definitions {
    operation=$1
    declare -a args=("$@")
    COMMAND_ARGS="${args[@]:1}"

    case ${operation} in
        by-image)
            task_definition_by_image ${COMMAND_ARGS}
            ;;
        summaries)
            task_definition_summaries ${COMMAND_ARGS}
            ;;
        version)
            task_definition_version_details ${COMMAND_ARGS}
            ;;
        all)
            task_definition_versions ${COMMAND_ARGS}
            ;;
        latest)
            task_definition_latest ${COMMAND_ARGS}
            ;;
        *)
            error "Unrecognized action ${operation}"
            exit 1
            ;;
    esac

}

# ----- Utitities for log groups -----------

function create_log_group {
    log_group="$1"
    if [[ -z "${log_group}" ]]; then log_group="${PACKAGE_DEPLOYABLE_TASK_DEFINITION}"; fi

    echo "$(aws_exec logs create-log-group --log-group-name "${log_group}")"
}


# ----------S3 Utilities ----------------------
#

function get_s3 {
    bucket="$1"
    key="$2"
    outfile="$(mktemp)"
    aws_response="$(aws_exec s3api get-object --bucket "${bucket}" --key "${key}" "${outfile}"  2>/dev/null )"
    if [[ ! -z "${aws_response}" ]]
    then
        echo "$(cat "${outfile}")"
    else
        echo "{}"
    fi
    rm -f "${outfile}"
}

function put_s3 {
    bucket="$1"
    key="$2"
    declare -a args=("$@")
    content="${args[@]:2}"

    outfile="$(mktemp)"
    echo "${content}" > "${outfile}"

    aws_response="$(aws_exec s3api put-object --bucket "${bucket}" --key "${key}" --body "${outfile}"  2>/dev/null )"
    echo ${aws_response}
    rm -f "${outfile}"

}

function create_ssh_key {
    organization_key="$1"
    if [[ -z "${organization_key}" ]]; then organization_key='default'; fi
    key_label=$2
    if [[ -z "${key_label}" ]]; then key_label='polaris-scan@exathink.com'; fi

    s3_config_bucket="$3"
    if [[ -z "${s3_config_bucket}" ]]; then s3_config_bucket="${S3_CONFIG_BUCKET}"; fi




    message "Generating ssh keys.."
    tempdir="$(mktemp -d)"
    ssh-keygen -q -t rsa -b 4096 -C "${key_label}" -f "${tempdir}/k" -N ""
    ssh-keygen -E md5 -lf "${tempdir}/k" > "${tempdir}/fingerprint"

    s3_public_key_dir="polaris-ssh-public/${organization_key}"
    message "Uploading public key and fingerprint to ${s3_public_key_dir}"
    aws_response="$(aws_exec s3api put-object \
                        --bucket "${s3_config_bucket}" \
                        --key  "${s3_public_key_dir}/k.pub"\
                        --body "${tempdir}/k.pub"\
                        --server-side-encryption AES256
                           )"
    echo "Key Uploaded: ${aws_response}"
    aws_response="$(aws_exec s3api put-object \
                        --bucket "${s3_config_bucket}" \
                        --key "${s3_public_key_dir}/fingerprint" \
                        --body "${tempdir}/fingerprint" \
                        --server-side-encryption AES256
                         )"
    echo "Fingeprint Uploaded: ${aws_response}"
    s3_private_key_dir="polaris-ssh/${organization_key}"
    message "Uploading private key and fingerprint to ${s3_private_key_dir}"
    aws_response="$(aws_exec s3api put-object \
                        --bucket "${s3_config_bucket}" \
                        --key "${s3_private_key_dir}/k" \
                        --body "${tempdir}/k"\
                        --server-side-encryption AES256
                           )"
    echo "Key Uploaded: ${aws_response}"
    aws_response="$(aws_exec s3api put-object \
                        --bucket "${s3_config_bucket}" \
                        --key "${s3_private_key_dir}/fingerprint" \
                        --body "${tempdir}/fingerprint" \
                        --server-side-encryption AES256
                         )"
    echo "Fingerprint Uploaded: ${aws_response}"
    echo "${tempdir}"

}

function fetch_ssh_public_key {
    organization_key="$1"
    if [[ -z "${organization_key}" ]]; then organization_key='default'; fi

    s3_config_bucket="$2"
    if [[ -z "${s3_config_bucket}" ]]; then s3_config_bucket="${S3_CONFIG_BUCKET}"; fi

    tempdir="$(mktemp -d)"
    s3_public_key_dir="polaris-ssh-public/${organization_key}"
    aws_response="$(aws_exec s3api get-object \
                        --bucket "${s3_config_bucket}" \
                        --key "${s3_public_key_dir}/k.pub" \
                        "${tempdir}/k.pub" \
                         )"
    echo "${tempdir}/k.pub"
}