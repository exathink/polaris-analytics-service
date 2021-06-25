# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from marshmallow import fields

from polaris.messaging.messages import Command
from polaris.messaging.types import CommitSummary


class ResolveTeamsForCommits(Command):
    message_type = 'commands.resolve_teams_for_commits'

    organization_key = fields.String(required=True)
    repository_key = fields.String(required=True)
    new_commits = fields.Nested(CommitSummary, many=True, required=True)



