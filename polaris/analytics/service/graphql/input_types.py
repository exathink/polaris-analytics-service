# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import graphene

from .interfaces import UserInfo
from polaris.analytics.service.graphql.interfaces import WorkTrackingIntegrationType


class WorkTrackingSettingsInput(graphene.InputObjectType):
    enabled = graphene.Boolean(required=True, default_value=False)
    providers = graphene.List(WorkTrackingIntegrationType, required=True, default_value=[])


class AccountProfileInput(graphene.InputObjectType):
    work_tracking = graphene.Field(WorkTrackingSettingsInput, required=True)


class UserInfoInput(graphene.InputObjectType):
    first_name = graphene.String(required=True)
    last_name = graphene.String(required=True)
    email = graphene.String(required=True)