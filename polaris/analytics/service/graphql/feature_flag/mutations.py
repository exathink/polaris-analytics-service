# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
import graphene

from polaris.analytics.db import api as db_api
from polaris.analytics.db.enums import FeatureFlagScope
from ..feature_flag import FeatureFlag

logger = logging.getLogger('polaris.analytics.graphql')
FeatureFlagScope = graphene.Enum.from_enum(FeatureFlagScope)


class CreateFeatureFlagInput(graphene.InputObjectType):
    name = graphene.String(required=True)


class EnableFeatureFlagModel(graphene.InputObjectType):
    scope = FeatureFlagScope(required=True)
    scope_key = graphene.String(required=True)
    enabled = graphene.Boolean(required=True)

class UpdateEnablementModel(graphene.InputObjectType):
    scope_key = graphene.String(required=True)
    enabled = graphene.Boolean(required=True)

class EnableFeatureFlagInput(graphene.InputObjectType):
    feature_flag_key = graphene.String(required=True)
    enablements = graphene.List(EnableFeatureFlagModel)

class UpdateEnablementsStatusInput(graphene.InputObjectType):
    feature_flag_key = graphene.String(required=True)
    enablements = graphene.List(UpdateEnablementModel)


class CreateFeatureFlag(graphene.Mutation):
    class Arguments:
        create_feature_flag_input = CreateFeatureFlagInput(required=True)

    success = graphene.Boolean()
    error_message = graphene.String()
    feature_flag = FeatureFlag.Field(key_is_required=False)

    def mutate(self, info, create_feature_flag_input):
        result = db_api.create_feature_flag(create_feature_flag_input)
        feature_flag = FeatureFlag.resolve_field(info, key=result.get('feature_flag').key) if result[
            'success'] else None
        resolved = CreateFeatureFlag(
            success=result['success'],
            error_message=result.get('message'),
            feature_flag=feature_flag
        )
        return resolved


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

class UpdateEnablementsStatus(graphene.Mutation):
    class Arguments:
        update_enablements_status_input = UpdateEnablementsStatusInput(required=True)

    success = graphene.Boolean()
    error_message = graphene.String()

    def mutate(self, info, update_enablements_status_input):
        result = db_api.update_enablements_status(update_enablements_status_input)
        return UpdateEnablementsStatus(
            success=result['success'],
            error_message=result.get('message')
        )


class FeatureFlagMutationsMixin:
    create_feature_flag = CreateFeatureFlag.Field()
    enable_feature_flag = EnableFeatureFlag.Field()
    update_enablements_status = UpdateEnablementsStatus.Field()
