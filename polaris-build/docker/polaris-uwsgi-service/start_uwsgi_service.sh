#!/usr/bin/env bash

if [[ -z "${MOUNT_PATH}" || -z "${MOUNT_APP}" ]]
then
    echo "Aborting startup: The environment variables 'MOUNT_PATH' and 'MOUNT_APP' must be provided and specify an app to mount."
    exit 1
fi
echo "Starting uwsgi service.."
echo "Mount app: ${MOUNT_APP}"
echo "Mount path: ${MOUNT_PATH}"

export APP_MOUNT="${MOUNT_PATH}=${MOUNT_APP}"


if [[ -z "${SERVICE_PORT}" ]]; then SERVICE_PORT = 8000; fi
echo "Service Port: ${SERVICE_PORT}"

if [[ -z "${STATS_PORT}" ]]; then STATS_PORT = 9190; fi
echo "Stats Port: ${STATS_PORT}"

if [[ -z "${PROCESSES}" ]]; then PROCESSES = 1; fi
echo "Processes: ${PROCESSES}"

if [[ -z "${THREADS}" ]]; then THREADS = 1; fi
echo "Threads: ${THREADS}"

uwsgi --http 0.0.0.0:${SERVICE_PORT} \
      --stats 0.0.0.0:${STATS_PORT} \
      --stats-http \
      --processes ${PROCESSES} \
      --threads ${THREADS} \
      --uid uwsgi \
      --gid uwsgi \
      --manage-script-name \
      --mount \
      ${APP_MOUNT} \
      --lazy-apps