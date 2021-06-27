from polaris.messaging.message_factory import register_messages
from .contributor_team_assignment_changed import ContributorTeamAssignmentsChanged

__exported__ = [
    ContributorTeamAssignmentsChanged
]

register_messages(__exported__)

__all__ = [
    export.__name__ for export in __exported__
]