# JQ Filters for output of the aws ecs describe-services command
# http://docs.aws.amazon.com/cli/latest/reference/ecs/describe-tasks.html

if $show == "summary" then
    {
        tasks: .tasks |
                      map({
                       group,
                       lastStatus,
                       startedAt: .startedAt | todate,
                       containers: (.containers | map ({
                        name,
                        lastStatus,
                        networkBindings: (.networkBindings| map({containerPort, hostPort})),
                        containerArn,
                        })),
                       taskArn,
                       ec2InstanceArn: .containerInstanceArn
                       }),
        failures
    }
elif $show == "taskArns" then
    .tasks | map(.taskArn)
elif $show == "taskIds" then
    .tasks | map(.taskArn | capture("task/(?<taskId>.*)")) | map(.taskId)
elif $show == "ec2Instances" then
    .tasks | map(.containerInstanceArn)
elif $show == "stable" then
    .tasks | map(select(.lastStatus == .desiredStatus))
elif $show == "stableTaskIds" then
    .tasks | map(select(.lastStatus == .desiredStatus)) | map(.taskArn | capture("task/(?<taskId>.*)")) | map(.taskId)
else
    .
end


