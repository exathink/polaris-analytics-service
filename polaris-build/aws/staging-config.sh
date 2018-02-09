#!/usr/bin/env bash

message "AWS_ENV: ${AWS_ENV}"

# Cluster names
export CLUSTER=polaris

# Service-Cluster Mapping
declare -A SERVICE_CLUSTER

SERVICE_CLUSTER['polaris-admin-service']="${CLUSTER}"
SERVICE_CLUSTER['polaris-auth-service']="${CLUSTER}"
SERVICE_CLUSTER['polaris-repos-intake-service']="${CLUSTER}"
SERVICE_CLUSTER['polaris-repos-sync-agent']="${CLUSTER}"
SERVICE_CLUSTER['polaris-repos-import-agent']="${CLUSTER}"
SERVICE_CLUSTER['polaris-repos-update-agent']="${CLUSTER}"
SERVICE_CLUSTER['polaris-repos-update-large-agent']="${CLUSTER}"

#Log-Group Mappings
declare -A LOG_GROUP
LOG_GROUP['polaris-admin-service']="polaris-admin-service"
LOG_GROUP['polaris-auth-service']="polaris-auth-service"
LOG_GROUP['polaris-repos-intake-service']="polaris-repos-intake-service"
LOG_GROUP['polaris-repos-sync-agent']="polaris-repos-sync-agent"
LOG_GROUP['polaris-repos-import-agent']="polaris-repos-import-agent"
LOG_GROUP['polaris-repos-update-large-agent']="polaris-repos-update-large-agent"

#S3 Config buckets
S3_CONFIG_BUCKET='exathink.polaris.services.staging'


# Lookup API

function get_cluster {
    service=$1
    echo ${SERVICE_CLUSTER[${service}]}
}

function get_log_group {
    service=$1
    log_group=${LOG_GROUP[${service}]}
    if [[ -z "${log_group}" ]]; then echo "${service}"; else echo "${log_group}"; fi
}

function get_service_secrets_url {
    service=$1
    echo "s3://${S3_CONFIG_BUCKET}/${service}/secrets.json"
}

function get_configs_object_key {
    service=$1
    if [[ ! -z "${service}" ]]
    then
        echo "${service}/service-config.json"
    else
        error "get_secrets_object_key must specify a service"
        exit 1
    fi
}