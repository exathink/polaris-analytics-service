# -*- coding: utf-8 -*-


import uuid
import pytest

from datetime import datetime, timedelta
from graphene.test import Client
from polaris.analytics.service.graphql import schema

from test.fixtures.graphql import *

def get_trend_date(timestamp, days):
    return str((timestamp + timedelta(days=days)).date())

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


class TestRepositoryPullRequests:

    @pytest.fixture()
    def setup(self, api_pull_requests_import_fixture):
        organization, project, repositories, work_items_source, work_items_common, pull_requests_common = api_pull_requests_import_fixture
        api_helper = PullRequestImportApiHelper(organization, repositories, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4(),
                name=f'Issue {i}',
                display_id='1000',
                state='open',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )
            for i in range(0, 1)
        ]

        pull_requests = [
            dict(
                repository_id=repositories['alpha'].id,
                key=uuid.uuid4(),
                source_id=f'100{i}',
                source_branch='1000',
                source_repository_id=repositories['alpha'].id,
                title="Another change. Fixes issue #1000",
                created_at=start_date,
                updated_at=start_date,
                end_date=None,
                **pull_requests_common
            )
            for i in range(0, 3)
        ]

        yield Fixture(
            project=project,
            api_helper=api_helper,
            start_date=start_date,
            work_items=work_items,
            pull_requests=pull_requests,
            repository=repositories['alpha']
        )

    class TestPullRequestsConnection:

        def it_returns_the_pull_requests_for_the_repository(self, setup):
            fixture = setup

            api_helper = fixture.api_helper
            #  Import PRs
            api_helper.import_pull_requests(fixture.pull_requests, fixture.repository)

            client = Client(schema)

            query = """
                    query getRepositoryPullRequests($key:String!) {
                        repository(key: $key) {
                            pullRequests(interfaces: [NamedNode, PullRequestInfo, BranchRef]) {
                                edges {
                                    node {
                                        id
                                        name
                                        key
                                        age
                                        state
                                        createdAt
                                        endDate
                                        repositoryKey
                                        repositoryName
                                        branchName 
                                    }
                                }
                            }
                        }
                    }
                    """

            result = client.execute(query, variable_values=dict(
                key=fixture.repository.key
            ))

            assert not 'errors' in result
            assert result['data']
            assert len(result['data']['repository']['pullRequests']['edges']) == 3

        def it_respects_the_active_only_filter(self, setup):
            fixture = setup

            api_helper = fixture.api_helper

            # update one of the PRs to be merged
            merged_pr = fixture.pull_requests[0]
            merged_pr['state'] = 'merged'
            merged_pr['end_date'] = datetime.utcnow()

            api_helper.import_pull_requests(fixture.pull_requests, fixture.repository)

            client = Client(schema)

            query = """
                    query getRepositoryPullRequests($key:String!) {
                        repository(key: $key) {
                            pullRequests(interfaces: [NamedNode, PullRequestInfo, BranchRef], activeOnly: true) {
                                edges {
                                    node {
                                        id
                                        name
                                        key
                                        age
                                        state
                                        createdAt
                                        endDate
                                        repositoryKey
                                        repositoryName
                                        branchName 
                                    }
                                }
                            }
                        }
                    }
                    """

            result = client.execute(query, variable_values=dict(
                key=fixture.repository.key
            ))

            assert not 'errors' in result
            assert result['data']
            edges = result['data']['repository']['pullRequests']['edges']
            assert len(edges) == 2
            # Should filter out the closed PR
            assert str(fixture.pull_requests[0]['key']) not in [
                edge['node']['key']
                for edge in edges
            ]

        def it_reports_the_pull_requests_within_the_closed_within_days_filter(self, setup):
            fixture = setup

            api_helper = fixture.api_helper

            # update one of the PRs to be merged in the last day
            merged_pr = fixture.pull_requests[0]
            merged_pr['state'] = 'merged'
            merged_pr['end_date'] = datetime.utcnow() - timedelta(days=1)

            api_helper.import_pull_requests(fixture.pull_requests, fixture.repository)

            client = Client(schema)

            query = """
                    query getRepositoryPullRequests($key:String!, $days: Int!) {
                        repository(key: $key) {
                            pullRequests(interfaces: [NamedNode, PullRequestInfo, BranchRef], closedWithinDays: $days) {
                                edges {
                                    node {
                                        id
                                        name
                                        key
                                        age
                                        state
                                        createdAt
                                        endDate
                                        repositoryKey
                                        repositoryName
                                        branchName 
                                    }
                                }
                            }
                        }
                    }
                    """

            result = client.execute(query, variable_values=dict(
                key=fixture.repository.key,
                days=2
            ))

            assert not 'errors' in result
            assert result['data']
            edges = result['data']['repository']['pullRequests']['edges']
            assert len(edges) == 1
            # Should filter out the closed PR
            assert str(fixture.pull_requests[0]['key']) in [
                edge['node']['key']
                for edge in edges
            ]

        def it_excludes_the_pull_requests_outside_the_closed_within_days_filter(self, setup):
            fixture = setup

            api_helper = fixture.api_helper

            # update one of the PRs to be merged in the last day
            merged_pr = fixture.pull_requests[0]
            merged_pr['state'] = 'merged'
            merged_pr['end_date'] = datetime.utcnow() - timedelta(days=2)

            api_helper.import_pull_requests(fixture.pull_requests, fixture.repository)

            client = Client(schema)

            query = """
                    query getRepositoryPullRequests($key:String!, $days: Int!) {
                        repository(key: $key) {
                            pullRequests(interfaces: [NamedNode, PullRequestInfo, BranchRef], closedWithinDays: $days) {
                                edges {
                                    node {
                                        id
                                        name
                                        key
                                        age
                                        state
                                        createdAt
                                        endDate
                                        repositoryKey
                                        repositoryName
                                        branchName 
                                    }
                                }
                            }
                        }
                    }
                    """

            result = client.execute(query, variable_values=dict(
                key=fixture.repository.key,
                days=1
            ))

            assert not 'errors' in result
            assert result['data']
            edges = result['data']['repository']['pullRequests']['edges']
            assert len(edges) == 0

    class TestPullRequestsMetricsTrends:

        @pytest.fixture
        def setup(self, setup):
            fixture = setup

            query = """
                    query getRepositoryPullRequestsMetricsTrends(
                        $key:String!, 
                        $days: Int!, 
                        $samplingFrequency: Int!,
                        $measurementWindow: Int!,
                        $targetPercentile:Float!
    
                        ) {
                        repository(
                            key:$key, 
                            interfaces: [PullRequestMetricsTrends],
                            pullRequestMetricsTrendsArgs:{
                                  days: $days,
                                  samplingFrequency:$samplingFrequency,
                                  measurementWindow:$measurementWindow,
                                  pullRequestAgeTargetPercentile: $targetPercentile,
                                  metrics:[
                                    min_age,
                                    avg_age,
                                    percentile_age,
                                    max_age, 
                                    total_closed,
                                    total_open
                                  ]
                            }
                        ) {
                              pullRequestMetricsTrends {
                                measurementDate
                                minAge
                                avgAge
                                percentileAge
                                maxAge
                                totalClosed
                                totalOpen
                              }
                        }
                    }
                    """

            api_helper = fixture.api_helper

            prs = fixture.pull_requests
            now = datetime.utcnow()

            # setup a distribution of prs
            # time to close = 7 days
            prs[0]['created_at'] = now - timedelta(days=10)
            prs[0]['state'] = 'merged'
            prs[0]['end_date'] = now - timedelta(days=2)

            # time to close = 4 days
            prs[1]['created_at'] = now - timedelta(days=5)
            prs[1]['state'] = 'merged'
            prs[1]['end_date'] = now - timedelta(days=1)

            # third pr in fixture is open.

            #  Import PRs
            api_helper.import_pull_requests(fixture.pull_requests, fixture.repository)

            yield Fixture(
                parent=fixture,
                query=query,
                now=now
            )

        def it_computes_trends_at_daily_granulariy(self, setup):
            fixture = setup
            client = Client(schema)
            now = fixture.now

            result = client.execute(fixture.query, variable_values=dict(
                key=fixture.repository.key,
                days=10,
                samplingFrequency=1,
                measurementWindow=1,
                targetPercentile=0.5
            ))

            assert not 'errors' in result
            assert result['data']
            trends = result['data']['repository']['pullRequestMetricsTrends']
            assert trends == [
                {'measurementDate': get_trend_date(now, days=0), 'minAge': 0.0, 'avgAge': 0.0, 'percentileAge': 0.0,
                 'maxAge': 0.0,
                 'totalClosed': 0, 'totalOpen': 0},
                {'measurementDate': get_trend_date(now, days=-1), 'minAge': 4.0, 'avgAge': 4.0, 'percentileAge': 4.0,
                 'maxAge': 4.0,
                 'totalClosed': 1, 'totalOpen': 0},
                {'measurementDate': get_trend_date(now, days=-2), 'minAge': 8.0, 'avgAge': 8.0, 'percentileAge': 8.0,
                 'maxAge': 8.0,
                 'totalClosed': 1, 'totalOpen': 0},
                {'measurementDate': get_trend_date(now, days=-3), 'minAge': 0.0, 'avgAge': 0.0, 'percentileAge': 0.0,
                 'maxAge': 0.0,
                 'totalClosed': 0, 'totalOpen': 0},
                {'measurementDate': get_trend_date(now, days=-4), 'minAge': 0.0, 'avgAge': 0.0, 'percentileAge': 0.0,
                 'maxAge': 0.0,
                 'totalClosed': 0, 'totalOpen': 0},
                {'measurementDate': get_trend_date(now, days=-5), 'minAge': 0.0, 'avgAge': 0.0, 'percentileAge': 0.0,
                 'maxAge': 0.0,
                 'totalClosed': 0, 'totalOpen': 0},
                {'measurementDate': get_trend_date(now, days=-6), 'minAge': 0.0, 'avgAge': 0.0, 'percentileAge': 0.0,
                 'maxAge': 0.0,
                 'totalClosed': 0, 'totalOpen': 0},
                {'measurementDate': get_trend_date(now, days=-7), 'minAge': 0.0, 'avgAge': 0.0, 'percentileAge': 0.0,
                 'maxAge': 0.0,
                 'totalClosed': 0, 'totalOpen': 0},
                {'measurementDate': get_trend_date(now, days=-8), 'minAge': 0.0, 'avgAge': 0.0, 'percentileAge': 0.0,
                 'maxAge': 0.0,
                 'totalClosed': 0, 'totalOpen': 0},
                {'measurementDate': get_trend_date(now, days=-9), 'minAge': 0.0, 'avgAge': 0.0, 'percentileAge': 0.0,
                 'maxAge': 0.0,
                 'totalClosed': 0, 'totalOpen': 0},
                {'measurementDate': get_trend_date(now, days=-10), 'minAge': 0.0, 'avgAge': 0.0, 'percentileAge': 0.0,
                 'maxAge': 0.0,
                 'totalClosed': 0, 'totalOpen': 0}
            ]

        def it_computes_trends_at_weekly_granulariy(self, setup):
            fixture = setup
            client = Client(schema)
            now = fixture.now

            result = client.execute(fixture.query, variable_values=dict(
                key=fixture.repository.key,
                days=10,
                samplingFrequency=7,
                measurementWindow=7,
                targetPercentile=0.5
            ))

            assert not 'errors' in result
            assert result['data']
            trends = result['data']['repository']['pullRequestMetricsTrends']
            assert trends == [
                {'measurementDate': get_trend_date(now, days=0), 'minAge': 4.0, 'avgAge': 6.0, 'percentileAge': 4.0,
                 'maxAge': 8.0, 'totalClosed': 2, 'totalOpen': 0},
                {'measurementDate': get_trend_date(now, days=-7), 'minAge': 0.0, 'avgAge': 0.0, 'percentileAge': 0.0,
                 'maxAge': 0.0,'totalClosed': 0, 'totalOpen': 0}
            ]
