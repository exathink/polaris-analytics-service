# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from marshmallow import fields, Schema

from polaris.messaging.messages import Message


class ContributorTeamAssignment(Schema):
    contributor_key = fields.String(required=True)
    new_team_key = fields.String(required=True)
    capacity = fields.Float(required=False)


class ResolveWorkItemsForCommits(Message):
    message_type = 'contributors.team_assignment_changed'

    organization_key = fields.String(required=True)
    contributor_team_assignments = fields.Nested(ContributorTeamAssignment, many=True, required=True)
