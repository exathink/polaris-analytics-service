# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from marshmallow import Schema, fields


class WorkItemsSourceStateMap(Schema):

    source_id = fields.String(required=False, allow_none=True)


