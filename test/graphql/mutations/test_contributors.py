# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar



from test.fixtures.contributors import *

from graphene.test import Client
from polaris.analytics.service.graphql import schema


class TestUpdateContributorForContributorAlias:

    def it_returns_the_updated_aliases_when_successful(self, setup_commits_for_contributor_updates):
        client = Client(schema)
        query = """
            mutation updateAlias($contributorAliasMapping: ContributorAliasMapping! ){
                updateContributorForContributorAliases(
                    contributorAliasMapping: $contributorAliasMapping
                ){
                    updatedAliasKeys
                }
            }
        """
        result = client.execute(query, variable_values=dict(
            contributorAliasMapping=dict(
                contributorKey=joe_contributor_key,
                contributorAliasKeys=[joe_alt_contributor_key]
            )
        ))
        assert 'errors' not in result
        assert result['data']['updateContributorForContributorAliases']['updatedAliasKeys'] == [joe_alt_contributor_key]
