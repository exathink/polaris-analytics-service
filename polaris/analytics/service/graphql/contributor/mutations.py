# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
import logging
from polaris.analytics import api
from polaris.utils.exceptions import ProcessingException

logger = logging.getLogger('polaris.analytics.graphql')


class ContributorUpdatedInfo(graphene.InputObjectType):
    contributor_name = graphene.String(required=False)
    unlink_contributor_alias_keys = graphene.List(graphene.String, required=False)
    contributor_alias_keys = graphene.List(graphene.String, required=False)
    excluded_from_analysis = graphene.Boolean(required=False)


class ContributorAliasMapping(graphene.InputObjectType):
    contributor_key = graphene.String(required=True)
    updated_info = graphene.Field(ContributorUpdatedInfo, required=True)


class UpdateContributorAliasesStatus(graphene.ObjectType):
    contributor_key = graphene.String(required=True)
    success = graphene.Boolean(required=True)
    message = graphene.String(required=False)
    exception = graphene.String(required=False)


class UpdateContributor(graphene.Mutation):
    class Arguments:
        contributor_alias_mapping = ContributorAliasMapping(required=True)

    update_status = graphene.Field(UpdateContributorAliasesStatus, required=True)

    def mutate(self, info, contributor_alias_mapping):
        logger.info('Update ContributorForContributorAlias called')
        result = api.update_contributor(
            contributor_key=contributor_alias_mapping.get('contributor_key'),
            updated_info=contributor_alias_mapping.get('updated_info')
        )
        if result:
            return UpdateContributor(
                UpdateContributorAliasesStatus(
                    contributor_key=contributor_alias_mapping.get('contributor_key'),
                    success=result.get('success'),
                    message=result.get('message'),
                    exception=result.get('exception')
                )
            )
        else:
            raise ProcessingException(result.get('exception'))
