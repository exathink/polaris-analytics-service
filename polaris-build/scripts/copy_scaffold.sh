#!/usr/bin/env bash
if [[ ! -z "$1" ]]
then
    TARGET_DIR="$1"
    SCAFFOLD_DIR="${TARGET_DIR}/polaris-build/scaffold"

    declare -a files_to_copy=(install.sh package pytest.ini docker-compose.yml requirements.txt setup.py conftest.py .package-env .gitlab-ci.yml)
    for file in "${files_to_copy[@]}"; do
        if [ ! -f  "${TARGET_DIR}/${file}" ]
        then
            echo "Copying file ${file}"
            cp "${SCAFFOLD_DIR}/${file}" "${TARGET_DIR}"
        fi
    done

    declare -a dirs_to_copy=(test bin cli)
    for dir in "${dirs_to_copy[@]}"; do
        if [ ! -d  "${TARGET_DIR}/${dir}" ]
        then
            echo "Copying directory ${dir}"
            cp -r "${SCAFFOLD_DIR}/${dir}" "${TARGET_DIR}"
        fi
    done
else
    echo "Usage: copy_scaffold <target_directory>"
fi