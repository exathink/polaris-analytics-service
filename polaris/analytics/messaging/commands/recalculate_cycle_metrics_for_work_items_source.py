# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from marshmallow import fields

from polaris.messaging.messages import Command
from polaris.messaging.types import CommitSummary


class RecalculateCycleMetricsForWorkItemSource(Command):
    message_type = 'commands.recalculate_cycle_metrics_for_work_items_source'

    project_key = fields.String(required=True)
    work_items_source_key = fields.String(required=True)
    rebuild_delivery_cycles = fields.Boolean(required=True)



