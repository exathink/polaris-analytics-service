from enum import Enum
from polaris.common.enums import GithubWorkItemType, PivotalTrackerWorkItemType, JiraWorkItemType, \
    GitlabWorkItemType

class WorkItemType(Enum):
    story = 'story'
    bug = 'bug'
    task = 'task'
    sub_task = 'subtask'
    epic = 'epic'


class WorkItemsStateType(Enum):
    open = 'open'
    wip = 'wip'
    complete = 'complete'
    backlog = 'backlog'
    closed = 'closed'
    unmapped = 'unmapped'


class WorkItemsStateFlowType(Enum):
    active = 'active'
    waiting = 'waiting'
    terminal = 'terminal'

class WorkItemsStateReleaseStatusType(Enum):
    # These are release status for Define phase
    # deferred items are in the inactive backlog and ignored in the funnel and in the lead time calculations.
    deferred = 'deferred'
    # lead time clock starts when it enters the roadmap
    roadmap = 'roadmap'
    # days supply is calculated using the committed backlog.
    committed = 'committed'
    # These are release status for the code phase
    implementation = 'implementation'
    code_review = 'code_review'

    # These are release status for Ship/Code Phase
    testing = 'testing'
    integration = 'integration'
    approval = 'approval'
    deployable = 'deployable'
    releasable = 'releasable'
    # these are release status of the Ship/Closed Phase.
    deployed = 'deployed'
    released = 'released'
    validated = 'validated'
    abandoned = 'abandoned'
    terminal = 'terminal'

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
        GitlabWorkItemType.story.value,
        GitlabWorkItemType.enhancement.value
    ]

    task_types = [
        JiraWorkItemType.task.value,
        JiraWorkItemType.sub_task.value,
        PivotalTrackerWorkItemType.chore.value,
        GithubWorkItemType.issue.value,
        GithubWorkItemType.pull_request.value,
        GitlabWorkItemType.task.value,
        GitlabWorkItemType.issue.value
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

class WorkItemsImpedimentType(Enum):
    flagged = 'flagged'

