from enum import Enum


class WorkItemsStateType(Enum):
    open = 'open'
    wip = 'wip'
    complete = 'complete'
    backlog = 'backlog'
    closed = 'closed'

class FeatureFlagScope(Enum):
    account = 'account'
    user = 'user'
