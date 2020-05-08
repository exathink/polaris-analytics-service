# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


from marshmallow import fields

from polaris.messaging.messages import Message
from polaris.analytics.messaging.types import WorkItemsSourceStateMap


class WorkItemsSourceStateMapUpdated(Message):
    message_type = 'work_items.work_items_source_state_map_updated'

    work_items_source_state_map = fields.Nested(WorkItemsSourceStateMap)