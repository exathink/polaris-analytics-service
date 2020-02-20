from enum import Enum


class WorkItemsStateType(Enum):
    open = 'open'
    wip = 'wip'
    complete = 'complete'

class FeatureFlagScope(Enum):
    account = 'account'
    user = 'user'
