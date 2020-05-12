# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from marshmallow import Schema, fields
from .state_mapping import StateMapping


class WorkItemsSourceStateMap(Schema):

    work_items_source_key = fields.String(required=False, allow_none=True)
    state_maps = fields.Nested(StateMapping, many=True, required=True)



