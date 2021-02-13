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

    def it_returns_success_when_contributor_aliases_are_updated(self, setup_commits_for_contributor_updates):
        client = Client(schema)
        query = """
            mutation updateAlias($contributorAliasMapping: ContributorAliasMapping! ){
                updateContributorForContributorAliases(
                    contributorAliasMapping: $contributorAliasMapping
                ){
                    updateStatus 
                    {
                        contributorKey
                        success
                    }
                }
            }
        """
        result = client.execute(query, variable_values=dict(
            contributorAliasMapping=dict(
                contributorKey=joe_contributor_key,
                updatedInfo=dict(
                    contributorAliasKeys=[joe_alt_contributor_key]
                )
            )
        ))
        assert 'errors' not in result
        assert result['data']['updateContributorForContributorAliases']['updateStatus']['success']

    def it_returns_failure_message_when_contributor_not_found(self, setup_commits_for_contributor_updates):
        test_contributor_key =uuid.uuid4()
        client = Client(schema)
        query = """
                    mutation updateAlias($contributorAliasMapping: ContributorAliasMapping! ){
                        updateContributorForContributorAliases(
                            contributorAliasMapping: $contributorAliasMapping
                        ){
                            updateStatus 
                            {
                                contributorKey
                                success
                                message
                                exception
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            contributorAliasMapping=dict(
                contributorKey=test_contributor_key,
                updatedInfo=dict(
                    contributorAliasKeys=[joe_alt_contributor_key]
                )
            )
        ))
        assert 'errors' not in result
        assert not result['data']['updateContributorForContributorAliases']['updateStatus']['success']
        assert result['data']['updateContributorForContributorAliases']['updateStatus']['exception'] == f"Contributor with key: {test_contributor_key} was not found"

    def it_returns_failure_message_when_contributor_alias_not_found(self, setup_commits_for_contributor_updates):
        test_contributor_key = uuid.uuid4()
        client = Client(schema)
        query = """
                            mutation updateAlias($contributorAliasMapping: ContributorAliasMapping! ){
                                updateContributorForContributorAliases(
                                    contributorAliasMapping: $contributorAliasMapping
                                ){
                                    updateStatus 
                                    {
                                        contributorKey
                                        success
                                        message
                                        exception
                                    }
                                }
                            }
                        """
        result = client.execute(query, variable_values=dict(
            contributorAliasMapping=dict(
                contributorKey=joe_contributor_key,
                updatedInfo=dict(
                    contributorAliasKeys=[test_contributor_key]
                )
            )
        ))
        assert 'errors' not in result
        assert not result['data']['updateContributorForContributorAliases']['updateStatus']['success']
        assert result['data']['updateContributorForContributorAliases']['updateStatus']['exception'] == f"Could not find contributor alias with key {test_contributor_key}"
