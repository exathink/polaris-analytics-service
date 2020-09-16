# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import graphene


class AccountProfileInput(graphene.InputObjectType):
    defaultWorkTracking = graphene.String(required=False, default_value=None)


class UserInfoInput(graphene.InputObjectType):
    first_name = graphene.String(required=True)
    last_name = graphene.String(required=True)
    email = graphene.String(required=True)


class OrganizationProfileInput(graphene.InputObjectType):
    defaultWorkTracking = graphene.String(required=False, default_value=None)


class OrganizationInput(graphene.InputObjectType):
    name = graphene.String(required=True)
    profile = graphene.Field(OrganizationProfileInput, required=False)


class FlowMetricsSettings(graphene.InputObjectType):
    lead_time_target = graphene.Int(required=False)
    cycle_time_target = graphene.Int(required=False)
    response_time_confidence_target = graphene.Float(required=False)



