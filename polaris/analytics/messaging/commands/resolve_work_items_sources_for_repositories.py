# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from marshmallow import fields

from polaris.messaging.messages import Command
from polaris.messaging.types import RepositorySummary


class ResolveWorkItemsSourcesForRepositories(Command):
    message_type = 'commands.resolve_work_items_sources_for_repositories'

    organization_key = fields.String(required=True)
    repositories = fields.Nested(RepositorySummary, many=True, required=True)



