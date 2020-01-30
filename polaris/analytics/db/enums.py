from enum import Enum


class WorkItemsStateType(Enum):
    open = 'open'
    wip = 'wip'
    complete = 'complete'
