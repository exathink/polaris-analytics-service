from enum import Enum


class WorkItemsSourceStateType(Enum):
    open = 'open'
    wip = 'wip'
    complete = 'complete'
