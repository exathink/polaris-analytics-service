# -*- coding: utf-8 -*-



from test.fixtures.graphql import *
import pytest
from graphene.test import Client
from polaris.analytics.service.graphql import schema


class TestRepositoryCommitSummary:

    def it_implements_the_commit_summary_interface(self, commits_fixture):
        organization, _, repositories, _ = commits_fixture
        client = Client(schema)
        query = """
                    query getRepositoryCommitSummary($repository_key:String!) {
                        repository(key: $repository_key, interfaces: [CommitSummary]) {
                          id
                          key
                          earliestCommit
                          latestCommit
                          commitCount
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(repository_key=repositories['alpha'].key))
        assert 'data' in result
        assert result['data']['repository']
        assert result['data']['repository']['commitCount'] == 2
        assert result['data']['repository']['earliestCommit'] == get_date("2020-01-10").isoformat()
        assert result['data']['repository']['latestCommit'] == get_date("2020-02-05").isoformat()

