# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import pytest

from graphene.test import Client
from test.fixtures.graphql import *
from test.constants import *
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

class TestCommit:


    def it_resolves_commit_nodes_using_natural_commit_key(self, test_commits_fixture):
        test_repo, test_commits = test_commits_fixture
        client = Client(schema)


        test_commit = test_commits[0]

        # The natural commit key for commits is of the form "<repository_key>:<source_commit_id>"

        commit_key = f"{test_repo.key}:{test_commit['source_commit_id']}"
        expected_commit_key = f"{uuid.UUID(test_repo.key)}:{test_commit['source_commit_id']}"
        response = client.execute("""
                    query getCommitNode($commit_key:String!) {
                        commit(key: $commit_key) {
                            id
                            key
                        }
                    }
                """, variable_values=dict(commit_key=commit_key)
                                      )

        assert 'data' in response
        result = response['data']['commit']
        assert result
        assert result['id']
        assert result['key'] == expected_commit_key


    def it_implements_commit_info_interface_on_the_node(self, test_commits_fixture):
        test_repo, test_commits = test_commits_fixture
        client = Client(schema)


        test_commit = test_commits[0]

        # The natural commit key for commits is of the form "<repository_key>:<source_commit_id>"

        commit_key = f"{test_repo.key}:{test_commit['source_commit_id']}"
        response = client.execute("""
                    query getCommitNode($commit_key:String!) {
                        commit(key: $commit_key) {
                            ... on CommitInfo {
                                commitHash
                                repository
                                integrationType
                                repositoryUrl
                                repositoryKey
                                commitDate
                                committer
                                committerKey
                                authorDate
                                author
                                authorKey
                                commitMessage
                            }
                        }
                    }
                """, variable_values=dict(commit_key=commit_key)
                                      )

        assert 'data' in response
        result = response['data']['commit']
        assert result
        assert result['commitHash']
        assert result['repository']
        assert result['integrationType']
        assert result['repositoryUrl']
        assert result['repositoryKey']
        assert result['commitDate']
        assert result['committer']
        assert result['committerKey']
        assert result['authorDate']
        assert result['author']
        assert result['authorKey']
        assert result['commitMessage']



