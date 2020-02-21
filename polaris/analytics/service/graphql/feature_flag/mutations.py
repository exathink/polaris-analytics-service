# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
import graphene

from polaris.analytics.db import api as db_api
from ..feature_flag import FeatureFlag

logger = logging.getLogger('polaris.analytics.graphql')


class CreateFeatureFlagInput(graphene.InputObjectType):
    name = graphene.String(required=True)


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


class FeatureFlagMutationsMixin:
    create_feature_flag = CreateFeatureFlag.Field()
