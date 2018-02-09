#!/usr/bin/env bash

declare -x GITLAB_USER="polaris-scan-gl"
declare -x GITLAB_PAT="${polaris_scan_pat_gl}"
declare -x GITLAB_API="https://gitlab.com/api/v4"

function gitlab_update_ssh_public_key {
    organization_key="$1"
    if [[ -z "${organization_key}" ]]; then organization_key='default'; fi

    gitlab_user="$2"
    if [[ -z "${gitlab_user}" ]]; then gitlab_user="${GITLAB_USER}"; fi

    access_token="$3"
    if [[ -z "${access_token}" ]]; then access_token="${GITLAB_PAT}"; fi

    s3_config_bucket="$4"

    message "Fetching public key for ${organization_key}"
    public_key_file="$(fetch_ssh_public_key ${organization_key} ${s3_config_bucket})"
    echo "${public_key_file}"

    fingerprint="$(ssh-keygen -E md5 -lf ${public_key_file})"
    public_key="$(cat ${public_key_file})"

    message "Updating public key with fingerprint ${fingerprint} to GITLAB for user ${gitlab_user}"
    curl  --header "Private-Token: ${access_token}" \
          --data "title=${gitlab_user}@exathink.com" \
          --data "key=${public_key}" \
          "${GITLAB_API}/user/keys"
}