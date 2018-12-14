from polaris.messaging.message_factory import register_messages
from .resolve_commits_work_items import ResolveCommitsWorkItems

__exported__ = [
    ResolveCommitsWorkItems
]

register_messages(__exported__)

__all__ = [
    export.__name__ for export in __exported__
]