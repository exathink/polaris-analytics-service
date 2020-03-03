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


class FeatureFlagEnablementModel(graphene.InputObjectType):
    scope = FeatureFlagScope(required=True)
    scope_key = graphene.String(required=True)
    enabled = graphene.Boolean(required=True)


class UpdateFeatureFlagInput(graphene.InputObjectType):
    key = graphene.String(required=True)
    active = graphene.Boolean(required=False)
    enable_all = graphene.Boolean(required=False)
    enablements = graphene.List(FeatureFlagEnablementModel, required=False)


class CreateFeatureFlag(graphene.Mutation):
    class Arguments:
        create_feature_flag_input = CreateFeatureFlagInput(required=True)

    success = graphene.Boolean()
    error_message = graphene.String()
    feature_flag = FeatureFlag.Field(key_is_required=False)

    def mutate(self, info, create_feature_flag_input):
        result = db_api.create_feature_flag(create_feature_flag_input)
        resolved = CreateFeatureFlag(
            success=result['success'],
            error_message=result.get('message'),
            feature_flag=FeatureFlag.resolve_field(info, key=result.get('key')) if result['success'] else None
        )
        return resolved


class UpdateFeatureFlag(graphene.Mutation):
    class Arguments:
        update_feature_flag_input = UpdateFeatureFlagInput(required=True)

    success = graphene.Boolean()
    error_message = graphene.String()

    def mutate(self, info, update_feature_flag_input):
        result = db_api.update_feature_flag(update_feature_flag_input)
        return UpdateFeatureFlag(
            success=result['success'],
            error_message=result.get('message')
        )


class FeatureFlagMutationsMixin:
    create_feature_flag = CreateFeatureFlag.Field()
    update_feature_flag = UpdateFeatureFlag.Field()
