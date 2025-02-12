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





class ProjectCustomTypeMappingsChanged(Message):
    message_type = 'project.custom_type_mappings_changed'

    project_key = fields.String(required=True)
    work_items_source_keys = fields.List(fields.String(),  required=True)
