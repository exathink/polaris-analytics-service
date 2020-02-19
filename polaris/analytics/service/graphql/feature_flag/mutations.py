# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
import graphene

from polaris.analytics.db import api as db_api


logger = logging.getLogger('polaris.analytics.graphql')


class CreateFeatureFlagInput(graphene.InputObjectType):
    name = graphene.String(required=True)


class CreateFeatureFlag(graphene.Mutation):

    class Arguments:
        create_feature_flag_input = CreateFeatureFlagInput(required=True)

    success = graphene.Boolean()
    error_message = graphene.String()

    def mutate(self, info, create_feature_flag_input):
        result = db_api.create_feature_flag(create_feature_flag_input)
        return CreateFeatureFlag(
            success=result['success'],
            error_message=result.get('exception')
        )


class FeatureFlagMutationsMixin:
    create_feature_flag = CreateFeatureFlag.Field()