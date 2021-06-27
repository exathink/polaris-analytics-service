# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


from polaris.messaging.topics import AnalyticsTopic
from polaris.messaging.utils import publish
from polaris.analytics.messaging.messages import ContributorTeamAssignmentsChanged


def contributor_team_assignments_changed(organization_key, contributor_team_assignments, channel=None):
    message = ContributorTeamAssignmentsChanged(
        send=dict(
            organization_key=organization_key,
            contributor_team_assignments=contributor_team_assignments
        )
    )
    publish(
        AnalyticsTopic,
        message,
        channel=channel
    )
    return message
