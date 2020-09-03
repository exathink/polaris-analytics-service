from enum import Enum
from polaris.common.enums import GithubWorkItemType, PivotalTrackerWorkItemType, JiraWorkItemType


class WorkItemsStateType(Enum):
    open = 'open'
    wip = 'wip'
    complete = 'complete'
    backlog = 'backlog'
    closed = 'closed'


class FeatureFlagScope(Enum):
    account = 'account'
    user = 'user'


# Only types listed here will be included in cycle metrics calculations
WorkItemTypesToIncludeInCycleMetrics = {
    GithubWorkItemType.issue.value,
    GithubWorkItemType.pull_request.value,
    PivotalTrackerWorkItemType.story.value,
    JiraWorkItemType.bug.value,
    JiraWorkItemType.story.value,
    JiraWorkItemType.task.value,
    JiraWorkItemType.sub_task.value
}