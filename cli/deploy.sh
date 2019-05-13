#!/usr/bin/env bash
set -e
export DONT_PROMPT_FOR_CONFIRMATION=1
echo "deploying image"
package aws deploy-image

echo "deploying task definitions"
package aws deploy-task-definition polaris-analytics-service
package aws deploy-task-definition polaris-analytics-listener
package aws deploy-task-definition polaris-analytics-db-migrator

echo "Running migrations"
package aws run-task polaris-analytics-db-migrator

echo "Deploying Services.."
package aws deploy-services polaris-analytics-service polaris.service.auto-scaling-group polaris-analytics-service polaris-analytics-listener
