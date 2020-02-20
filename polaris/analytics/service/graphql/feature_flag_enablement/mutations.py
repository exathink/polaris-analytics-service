# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Maneet Sehgal

import logging
import graphene

from polaris.analytics.db import api as db_api

logger = logging.getLogger('polaris.analytics.graphql')

class FeatureFlagEnablement(graphene.InputObjectType):
    scope = graphene.String(required=True)
    scope_key = graphene.String(required=True)
    enabled = graphene.Boolean(required=True)

class CreateFeatureFlagEnablementInput(graphene.InputObjectType):
    feature_flag_key = graphene.String(required=True)
    feature_flag_enablements = graphene.List(FeatureFlagEnablement)

class CreateFeatureFlagEnablement(graphene.Mutation):

    class Arguments:
        create_feature_flag_enablements_input = CreateFeatureFlagEnablementInput(required=True)

    success = graphene.Boolean()
    error_message = graphene.String()

    def mutate(self, info, create_feature_flag_enablements_input):
        result = db_api.add_feature_flag_enablements(create_feature_flag_enablements_input)
        logger.info(result)
        return CreateFeatureFlagEnablement(
            success=result['success'],
            error_message=result.get('exception')
        )


class FeatureFlagEnablementMutationsMixin:
    create_feature_flag_enablement = CreateFeatureFlagEnablement.Field()