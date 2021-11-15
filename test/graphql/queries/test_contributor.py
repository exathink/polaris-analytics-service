# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from test.fixtures.graphql import *

from graphene.test import Client
from polaris.analytics.service.graphql import schema


@pytest.fixture
def test_commits_fixture(commits_fixture, org_repo_fixture):
    _, _, repositories = org_repo_fixture

    test_repo = repositories['alpha']

    test_commits = [
        dict(
            repository_id=test_repo.id,
            key=uuid.uuid4().hex,
            source_commit_id='XXXXX',
            commit_message="Another change. Fixes issue #1000",
            author_date=get_date("2018-12-03"),
            **commits_common_fields(commits_fixture)
        ),
        dict(
            repository_id=test_repo.id,
            key=uuid.uuid4().hex,
            source_commit_id='YYYYYY',
            commit_message="Another change. Fixes issue #2000",
            author_date=get_date("2018-12-03"),
            **commits_common_fields(commits_fixture)
        )
    ]
    create_test_commits(test_commits)
    yield test_repo, test_commits


class TestContributors:

    def it_resolves_named_nodes(self, test_commits_fixture):
        client = Client(schema)
        response = client.execute("""
                            query getContributorNode($contributor_key:String!) {
                                contributor(key: $contributor_key) {
                                    id
                                    name
                                    key
                                }
                            }
                        """, variable_values=dict(contributor_key=test_contributor_key)
                                  )

        assert 'data' in response
        result = response['data']['contributor']
        assert result
        assert result['id']
        assert result['key']
        assert result['name']




    def it_resolves_contributor_commits(self, test_commits_fixture):
        client = Client(schema)
        response = client.execute("""
                            query getContributorNode($contributor_key:String!) {
                                contributor(key: $contributor_key) {
                                    commits {
                                        edges {
                                            node {
                                                key
                                            }
                                        }
                                    }
                                }
                            }
                        """, variable_values=dict(contributor_key=test_contributor_key)
                                  )

        assert 'data' in response
        result = response['data']['contributor']
        assert result
        contributor_commits = result['commits']['edges']
        assert len(contributor_commits) == 2



    def it_implements_commit_summary_interface(self, api_import_commits_fixture):

        client = Client(schema)
        response = client.execute("""
                                    query getContributorNode($contributor_key:String!) {
                                        contributor(
                                            key: $contributor_key, 
                                            interfaces: [CommitSummary]) {
                                            ... on CommitSummary {
                                                earliestCommit
                                                latestCommit
                                                commitCount
                                            }
                                        }
                                    }
                                """, variable_values=dict(contributor_key=test_contributor_key)
                                  )

        assert 'data' in response
        result = response['data']['contributor']
        assert result
        assert result['earliestCommit']
        assert result['latestCommit']
        assert result['commitCount']

    # TODO: Complete the test suite for contributors. We need better fixtures and need to test interface/selectable
    # field resolvers.







