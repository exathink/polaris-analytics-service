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


class ContributorAliasMapping(graphene.InputObjectType):
    organization_key = graphene.String(required=True)
    contributor_key = graphene.String(required=True)
    contributor_alias_keys = graphene.List(graphene.String, required=True)


class UpdateContributorForContributorAliases(graphene.Mutation):
    class Arguments:
        contributor_alias_mapping = ContributorAliasMapping(required=True)

    updated_alias_keys = graphene.List(graphene.String)

    def mutate(self, info, contributor_alias_mapping):
        logger.info('Update ContributorForContributorAlias called')
        result = api.update_contributor_for_contributor_aliases(
                    organization_key=contributor_alias_mapping.get('organization_key'),
                    contributor_key=contributor_alias_mapping.get('contributor_key'),
                    contributor_alias_keys=contributor_alias_mapping.get('contributor_alias_keys')
                )
        if result['success']:
            return UpdateContributorForContributorAliases(
                    updated_alias_keys=result.get('updated_alias_keys')
            )
        else:
            raise ProcessingException(result.get('exception'))
