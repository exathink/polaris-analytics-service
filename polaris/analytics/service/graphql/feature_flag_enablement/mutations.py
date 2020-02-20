# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Maneet Sehgal

import logging
import graphene

from polaris.analytics.db import api as db_api
from polaris.analytics.db.enums import FeatureFlagScope
from ..feature_flag_enablement import FeatureFlagEnablement


logger = logging.getLogger('polaris.analytics.graphql')

FeatureFlagScope = graphene.Enum.from_enum(FeatureFlagScope)

class EnableFeatureFlagModel(graphene.InputObjectType):
    scope = FeatureFlagScope(required=True)
    scope_key = graphene.String(required=True)
    enabled = graphene.Boolean(required=True)

class EnableFeatureFlagInput(graphene.InputObjectType):
    feature_flag_key = graphene.String(required=True)
    enablements = graphene.List(EnableFeatureFlagModel)

class EnableFeatureFlag(graphene.Mutation):

    class Arguments:
        enable_feature_flag_input = EnableFeatureFlagInput(required=True)

    success = graphene.Boolean()
    error_message = graphene.String()

    def mutate(self, info, enable_feature_flag_input):
        result = db_api.enable_feature_flag(enable_feature_flag_input)
        return EnableFeatureFlag(
            success=result['success'],
            error_message=result.get('message')
        )


class FeatureFlagEnablementMutationsMixin:
    enable_feature_flag = EnableFeatureFlag.Field()