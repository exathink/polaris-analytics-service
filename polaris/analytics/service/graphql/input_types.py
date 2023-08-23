# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import graphene

from .interfaces import FlowMetricsSettings, AnalysisPeriods, WipInspectorSettings, ReleasesSettings


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


class FlowMetricsSettingsInput(FlowMetricsSettings, graphene.InputObjectType):
    pass


class AnalysisPeriodsInput(AnalysisPeriods, graphene.InputObjectType):
    pass


class WipInspectorSettingsInput(WipInspectorSettings, graphene.InputObjectType):
    pass

class ReleasesSettingsInput(ReleasesSettings, graphene.InputObjectType):
    pass