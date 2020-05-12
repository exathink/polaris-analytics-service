# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


from marshmallow import Schema, fields


class StateMapping(Schema):

    state = fields.String(required=True)
    state_type = fields.String(required=True)

