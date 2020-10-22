# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import pytest
from test.fixtures.graphql import *
from polaris.analytics.service.graphql import schema

from test.graphql.queries.projects.shared_testing_mixins import \
    TrendingWindowMeasurementDate
from graphene.test import Client

from operator import itemgetter

class TestProjectCapacityTrends:


    @pytest.yield_fixture
    def setup(self, org_repo_fixture, cleanup):
        organization, projects, repositories = org_repo_fixture
        contributors_fixture = []
        with db.create_session() as session:
            for i in range(0, 3):
                contributor_key = uuid.uuid4().hex
                contributor_name = str(i)
                contributor_id = session.connection.execute(
                    contributors.insert(
                        dict(
                            key=contributor_key,
                            name=contributor_name
                        )
                    )
                ).inserted_primary_key[0]

                contributor_alias_id = session.connection.execute(
                    contributor_aliases.insert(
                        dict(
                            source_alias=f'${i}@blow.com',
                            key=contributor_key,
                            name=contributor_name,
                            contributor_id=contributor_id,
                            source='vcs'
                        )
                    )
                ).inserted_primary_key[0]

                # this fixture is designed to make it easy to
                # setup a contributor as an author or a committer
                # of a commit
                contributors_fixture.append(
                    dict(
                        as_author=dict(
                            author_contributor_alias_id=contributor_alias_id,
                            author_contributor_key=contributor_key,
                            author_contributor_name=contributor_name
                        ),
                        as_committer=dict(
                            committer_contributor_alias_id=contributor_alias_id,
                            committer_contributor_key=contributor_key,
                            committer_contributor_name=contributor_name
                        )
                    )
                )

        commits_common = dict(
            commit_message=f"Another change. Fixes nothing",
            author_date=get_date("2018-12-03"),
            commit_date_tz_offset=0,
            author_date_tz_offset=0,
        )

        yield Fixture(
            organization=organization,
            project=projects['mercury'],
            repositories=repositories,

            contributor_a=contributors_fixture[0],
            contributor_b=contributors_fixture[1],
            contributor_c=contributors_fixture[2],

            commits_common=commits_common,
            create_test_commits=create_test_commits,
        )

    class TestTrendingWindows(
        TrendingWindowMeasurementDate
    ):

        @pytest.yield_fixture
        def setup(self, setup):
            fixture = setup
            measurements_query = """
                query getProjectCapacityTrends(
                    $project_key:String!, 
                    $days: Int!, 
                    $window: Int!,
                    $sample: Int,
                ) {
                    project(
                        key: $project_key, 
                        interfaces: [CapacityTrends], 
                        capacityTrendsArgs: {
                            days: $days,
                            measurementWindow: $window,
                            samplingFrequency: $sample,                            
                        }

                    ) {
                        capacityTrends {
                            measurementDate
                            measurementWindow
                            totalCommitDays
                            minCommitDays
                            maxCommitDays
                            avgCommitDays
                        }
                    }
                }
            """
            yield Fixture(
                parent=fixture,
                query=measurements_query,
                output_attribute='capacityTrends'
            )

    class TestCommitDays:
        @pytest.yield_fixture
        def setup(self, setup):
            fixture = setup
            test_repo = fixture.repositories['alpha']
            contributor_a = fixture.contributor_a
            contributor_b = fixture.contributor_b
            commits_common = fixture.commits_common

            start_date = datetime.utcnow() - timedelta(days=60)
            commits = []
            for i in range(0, 60):
                commits.extend([
                    dict(
                        key=uuid.uuid4().hex,
                        source_commit_id=uuid.uuid4().hex,
                        repository_id=test_repo.id,
                        commit_date=start_date + timedelta(days=i),
                        **contributor_a['as_author'],
                        **contributor_a['as_committer'],
                        **commits_common
                    ),
                    dict(
                        key=uuid.uuid4().hex,
                        source_commit_id=uuid.uuid4().hex,
                        repository_id=test_repo.id,
                        commit_date=start_date + timedelta(days=i),
                        **contributor_b['as_author'],
                        **contributor_b['as_committer'],
                        **commits_common
                    ),
                ])




            yield Fixture(
                parent=fixture,
                test_commits=commits,
            )

        class TestAggregateMetrics:

            @pytest.yield_fixture
            def setup(self, setup):
                fixture = setup

                measurements_query = """
                                        query getProjectCapacityTrends(
                                            $project_key:String!, 
                                            $days: Int!, 
                                            $window: Int!,
                                            $sample: Int,
                                        ) {
                                            project(
                                                key: $project_key, 
                                                interfaces: [CapacityTrends], 
                                                capacityTrendsArgs: {
                                                    days: $days,
                                                    measurementWindow: $window,
                                                    samplingFrequency: $sample,                            
                                                }

                                            ) {
                                                capacityTrends {
                                                    measurementDate
                                                    measurementWindow
                                                    totalCommitDays
                                                    minCommitDays
                                                    maxCommitDays
                                                    avgCommitDays
                                                }
                                            }
                                        }
                                    """
                yield Fixture(
                    parent=fixture,
                    query=measurements_query
                )


            def it_returns_an_empty_list_when_there_are_no_commits(self, setup):
                fixture = setup

                # Dont create commits before calling the query

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=30,
                    window=30,
                    sample=7,

                ))
                assert result['data']
                project = result['data']['project']
                assert len(project['capacityTrends']) == 0


            def it_returns_commit_days_metrics(self, setup):
                fixture = setup

                fixture.create_test_commits(fixture.test_commits)

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=30,
                    window=30,
                    sample=7,

                ))
                assert result['data']
                project = result['data']['project']
                assert len(project['capacityTrends']) == 5
                for capacityTrend in project['capacityTrends']:
                    assert capacityTrend['totalCommitDays'] == 60
                    assert capacityTrend['minCommitDays'] == 30
                    assert capacityTrend['maxCommitDays'] == 30
                    assert capacityTrend['avgCommitDays'] == 30





        class TestContributorDetail:

            @pytest.yield_fixture
            def setup(self, setup):
                fixture = setup

                measurements_query = """
                                        query getProjectCapacityTrends(
                                            $project_key:String!, 
                                            $days: Int!, 
                                            $window: Int!,
                                            $sample: Int,
                                        ) {
                                            project(
                                                key: $project_key, 
                                                interfaces: [CapacityTrends], 
                                                capacityTrendsArgs: {
                                                    days: $days,
                                                    measurementWindow: $window,
                                                    samplingFrequency: $sample,
                                                    showContributorDetail: true                            
                                                }

                                            ) {
                                                contributorDetail {
                                                    measurementDate
                                                    measurementWindow
                                                    contributorName
                                                    contributorKey
                                                    totalCommitDays
                                                }
                                            }
                                        }
                                    """
                yield Fixture(
                    parent=fixture,
                    query=measurements_query,

                )

            def it_returns_contributor_detail_metrics(self, setup):
                fixture = setup

                fixture.create_test_commits(fixture.test_commits)

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=30,
                    window=30,
                    sample=7,

                ))
                assert result['data']
                project = result['data']['project']
                assert len(project['contributorDetail']) == 10
                for contributorDetail in project['contributorDetail']:
                    assert contributorDetail['totalCommitDays'] == 30

                # two distinct authors in the set
                assert len(set(
                    contributorDetail['contributorKey']
                    for contributorDetail in project['contributorDetail']
                )) == 2
