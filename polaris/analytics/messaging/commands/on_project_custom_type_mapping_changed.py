# -*- coding: utf-8 -*-

#  Copyright (c) Exathink, LLC  2016-2023.
#  All rights reserved
#

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from marshmallow import fields, Schema

from polaris.messaging.messages import Message


class CustomTypeMapping(Schema):
   labels = fields.String(many=True, required=False)
   work_item_type = fields.String(required=True)



class ProjectCustomTypeMappingChanged(Message):
    message_type = 'project.custom_type_mapping_changed'

    project_key = fields.String(required=True)
    work_items_sources_keys = fields.String(many=True, required=True)
    custom_type_mappings = fields.Nested(CustomTypeMapping, many=True, allow_none=False)