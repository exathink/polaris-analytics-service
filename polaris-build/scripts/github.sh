#!/usr/bin/env bash

declare -x GITHUB_USER="polaris-scan"
declare -x GITHUB_PAT="${polaris_scan_pat_gh}"
declare -x GITHUB_API="https://api.github.com"

function github_update_ssh_public_key {
    organization_key="$1"
    if [[ -z "${organization_key}" ]]; then organization_key='default'; fi

    github_user="$2"
    if [[ -z "${github_user}" ]]; then github_user="${GITHUB_USER}"; fi

    access_token="$3"
    if [[ -z "${access_token}" ]]; then access_token="${GITHUB_PAT}"; fi

    s3_config_bucket="$4"

    message "Fetching public key for ${organization_key}"
    public_key="$(cat $(fetch_ssh_public_key ${organization_key} ${s3_config_bucket}))"
    post_data="{ \
                    \"title\": \"polaris-scan@exathink.com\",
                    \"key\": \"${public_key}\"
                }"
    echo ${post_data}
    message "Updating public key on github for user ${github_user}"
    curl  -u "${github_user}:${access_token}" --data "${post_data}" "${GITHUB_API}/user/keys"

}