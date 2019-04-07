# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
import logging
from polaris.analytics.api import update_contributor_for_contributor_aliases

logger = logging.getLogger('polaris.analytics.graphql')


class UpdateContributorForContributorAliases(graphene.Mutation):
    class Arguments:
        contributor_key = graphene.String(required=True)
        contributor_alias_keys = graphene.List(graphene.String, required=True)

    updated_alias_keys = graphene.List(graphene.String)

    def mutate(self, info, contributor_key, contributor_alias_keys):
        logger.info('Update ContributorForContributorAlias called')
        return UpdateContributorForContributorAliases(
                updated_alias_keys=update_contributor_for_contributor_aliases(contributor_key, contributor_alias_keys)
        )
