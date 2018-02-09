# JQ Filters for output of the aws ecs describe-services command
# http://docs.aws.amazon.com/cli/latest/reference/ecs/describe-services.html

if $show == "summary" then
    {
        service: .services[0] | {serviceName, taskDefinition, status, desiredCount, runningCount, pendingCount,
                                  deployments: .deployments | map( {
                                    taskDefinition, status, runningCount, desiredCount, pendingCount,
                                    createdAt: .createdAt | todate,
                                    updatedAt: .updatedAt | todate
                                  })},
        failures
    }
elif $show == "task-definition" then
    .services[0].taskDefinition
elif $show == "update-status" then
    .services[0] |
    {
        runningCount,
        desiredCount,
        pendingCount,
    }
    +
    (
        if (.deployments | length) > 1
        then
        {
            deployments: .deployments | map({
            taskDefinition, status, runningCount, desiredCount, pendingCount,
            createdAt: .createdAt | todate,
            updatedAt: .updatedAt | todate
            })
         }
         else
         {}
         end
    )
elif $show == "events" then
    .services[0] | {events} | .events | map({message, createdAt: .createdAt | todate})
elif $show == "deployments" then
    .services[0] | {deployments} | .deployments  | map({status, desiredCount, runningCount, pendingCount, taskDefinition, createdAt: .createdAt | todate, updatedAt: .updatedAt| todate})
else
    .
end


