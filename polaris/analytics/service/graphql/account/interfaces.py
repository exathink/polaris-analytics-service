# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from ..interfaces import WorkTrackingIntegrationType


class WorkTrackingSettings(graphene.InputObjectType):
    enabled = graphene.Boolean(required=True, default_value=False)
    providers = graphene.List(WorkTrackingIntegrationType, required=True, default_value=[])


class AccountProfile(graphene.InputObjectType):
    work_tracking = graphene.Field(WorkTrackingSettings, required=True)

