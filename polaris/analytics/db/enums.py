from enum import Enum
from polaris.common.enums import GithubWorkItemType, PivotalTrackerWorkItemType, JiraWorkItemType, \
    GitlabWorkItemType


class WorkItemsStateType(Enum):
    open = 'open'
    wip = 'wip'
    complete = 'complete'
    backlog = 'backlog'
    closed = 'closed'
    unmapped = 'unmapped'


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
    JiraWorkItemType.sub_task.value,
    GitlabWorkItemType.story.value,
    GitlabWorkItemType.enhancement.value,
    GitlabWorkItemType.issue.value,
    GitlabWorkItemType.task.value,
    GitlabWorkItemType.bug.value
}


class FlowTypes(Enum):
    feature = 'feature'
    task = 'task'
    defect = 'defect'
    other = 'other'


class WorkItemTypesToFlowTypes:
    feature_types = [
        JiraWorkItemType.story.value,
        PivotalTrackerWorkItemType.story.value,
        GithubWorkItemType.issue.value,
        GitlabWorkItemType.story.value,
        GitlabWorkItemType.enhancement.value,
        GitlabWorkItemType.issue.value
    ]

    task_types = [
        JiraWorkItemType.task.value,
        JiraWorkItemType.sub_task.value,
        PivotalTrackerWorkItemType.chore.value,
        GithubWorkItemType.pull_request.value,
        GitlabWorkItemType.task.value
    ]

    defect_types = [
        JiraWorkItemType.bug.value,
        GitlabWorkItemType.bug.value
    ]


all_work_item_types = [
  *WorkItemTypesToFlowTypes.feature_types,
  *WorkItemTypesToFlowTypes.task_types,
  *WorkItemTypesToFlowTypes.defect_types
]

num_feature_types = len(WorkItemTypesToFlowTypes.feature_types)
num_task_types = len(WorkItemTypesToFlowTypes.task_types)
num_defect_types = len(WorkItemTypesToFlowTypes.defect_types)


