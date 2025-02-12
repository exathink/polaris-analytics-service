# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import uuid
import pytest
from datetime import datetime
from graphene.test import Client
from polaris.common import db
from polaris.utils.collections import Fixture
from polaris.analytics.db.model import contributors, contributor_aliases
from polaris.analytics.service.graphql import schema
from test.graphql.queries.projects.shared_fixtures import exclude_repos_from_project
from test.fixtures.graphql import org_repo_fixture, commits_fixture, cleanup, \
    get_date, work_items_common, create_work_item_commits, create_project_work_items, \
    work_items_source_common, generate_n_work_items, generate_work_item

from test.fixtures.fixture_helpers import create_commit_sequence, create_commit_sequence_in_project

from test.graphql.queries.projects.shared_testing_mixins import \
    TrendingWindowMeasurementDate





class TestProjectCapacityTrends:
    @pytest.fixture
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
                            source='vcs',
                            # The last contributor we will make a robot.
                            robot=(i == 2),
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
            reposities=repositories,

            contributor_a=contributors_fixture[0],
            contributor_b=contributors_fixture[1],
            contributor_c_robot=contributors_fixture[2],

            commits_common=commits_common,

        )

    class TestTrendingWindows(
        TrendingWindowMeasurementDate
    ):

        @pytest.fixture
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

    class TestCapacityTrends:
        class TestAggregateMetrics:

            @pytest.fixture
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


            def it_returns_an_empty_list_when_there_are_commits_but_no_commits_mapped_to_the_project(self, setup):
                fixture = setup
                project = fixture.project
                repository = project.repositories[0]
                create_commit_sequence(
                    repository,
                    contributor=fixture.contributor_a,
                    end_date=datetime.utcnow(),
                    start_date_offset_days=60,
                    days_increment=1,
                    common_commit_fields=fixture.commits_common
                )


                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=project.key,
                    days=30,
                    window=30,
                    sample=7,

                ))
                assert result['data']
                project = result['data']['project']
                assert len(project['capacityTrends']) == 0


            def it_returns_aggregate_commit_days_when_there_are_commits_mapped_to_the_project(self, setup):
                fixture = setup

                # contributor_a_commits in project

                create_commit_sequence_in_project(
                    fixture.organization,
                    fixture.project,
                    fixture.project.repositories[0],
                    fixture.contributor_a,
                    end_date=datetime.utcnow(),
                    start_date_offset_days=60,
                    days_increment=1,
                    commits_common=fixture.commits_common,
                )
                # contributor_b_commits in project
                create_commit_sequence_in_project(
                    fixture.organization,
                    fixture.project,
                    fixture.project.repositories[1],
                    fixture.contributor_b,
                    end_date=datetime.utcnow(),
                    start_date_offset_days=60,
                    days_increment=1,
                    commits_common=fixture.commits_common,

                )

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

            def it_excludes_repositories_excluded_from_the_projects_in_capacity_calculations(self, setup):
                fixture = setup

                # contributor_a_commits in project

                create_commit_sequence_in_project(
                    fixture.organization,
                    fixture.project,
                    fixture.project.repositories[0],
                    fixture.contributor_a,
                    end_date=datetime.utcnow(),
                    start_date_offset_days=60,
                    days_increment=1,
                    commits_common=fixture.commits_common,
                )
                # contributor_b_commits in project
                create_commit_sequence_in_project(
                    fixture.organization,
                    fixture.project,
                    fixture.project.repositories[1],
                    fixture.contributor_b,
                    end_date=datetime.utcnow(),
                    start_date_offset_days=60,
                    days_increment=1,
                    commits_common=fixture.commits_common,

                )
                # exclude the first repo from the project.
                exclude_repos_from_project(fixture.project, [fixture.project.repositories[0]])

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
                    assert capacityTrend['totalCommitDays'] == 30
                    assert capacityTrend['minCommitDays'] == 30
                    assert capacityTrend['maxCommitDays'] == 30
                    assert capacityTrend['avgCommitDays'] == 30

        class TestContributorDetail:

            @pytest.fixture
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

            def it_returns_contributor_detail_metrics_when_there_are_commits_mapped_to_the_project(self, setup):
                fixture = setup

                create_commit_sequence_in_project(
                    fixture.organization,
                    fixture.project,
                    fixture.project.repositories[0],
                    fixture.contributor_a,
                    end_date=datetime.utcnow(),
                    start_date_offset_days=60,
                    days_increment=1,
                    commits_common=fixture.commits_common,
                )
                # contributor_b_commits in project
                create_commit_sequence_in_project(
                    fixture.organization,
                    fixture.project,
                    fixture.project.repositories[1],
                    fixture.contributor_b,
                    end_date=datetime.utcnow(),
                    start_date_offset_days=60,
                    days_increment=1,
                    commits_common=fixture.commits_common,

                )
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

            def it_excludes_contributors_from_excluded_repos(self, setup):
                fixture = setup

                create_commit_sequence_in_project(
                    fixture.organization,
                    fixture.project,
                    fixture.project.repositories[0],
                    fixture.contributor_a,
                    end_date=datetime.utcnow(),
                    start_date_offset_days=60,
                    days_increment=1,
                    commits_common=fixture.commits_common,
                )
                # contributor_b_commits in project
                create_commit_sequence_in_project(
                    fixture.organization,
                    fixture.project,
                    fixture.project.repositories[1],
                    fixture.contributor_b,
                    end_date=datetime.utcnow(),
                    start_date_offset_days=60,
                    days_increment=1,
                    commits_common=fixture.commits_common,

                )

                exclude_repos_from_project(fixture.project, [fixture.project.repositories[0]])

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=30,
                    window=30,
                    sample=7,

                ))
                assert result['data']
                project = result['data']['project']
                # only one contributor from non-excluded repos.
                assert len(project['contributorDetail']) == 5
                for contributorDetail in project['contributorDetail']:
                    assert contributorDetail['totalCommitDays'] == 30

                # only one distinct author in the set. other is in an excluded repo.
                assert len(set(
                    contributorDetail['contributorKey']
                    for contributorDetail in project['contributorDetail']
                )) == 1


    class TestRobotFiltering:
        class TestAggregateMetrics:

            @pytest.fixture
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



            def it_filters_out_commits_with_robots_as_author(self, setup):
                fixture = setup

                # contributor_a_commits with robot as author in project

                create_commit_sequence_in_project(
                    fixture.organization,
                    fixture.project,
                    fixture.project.repositories[0],
                    fixture.contributor_a,
                    end_date=datetime.utcnow(),
                    start_date_offset_days=60,
                    days_increment=1,
                    commits_common=fixture.commits_common,
                    author=fixture.contributor_c_robot
                )
                # contributor_b_commits in project as author and committer
                create_commit_sequence_in_project(
                    fixture.organization,
                    fixture.project,
                    fixture.project.repositories[1],
                    fixture.contributor_b,
                    end_date=datetime.utcnow(),
                    start_date_offset_days=60,
                    days_increment=1,
                    commits_common=fixture.commits_common,
                )

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

        class TestContributorDetail:

            @pytest.fixture
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

            def it_filters_out_commits_with_robots_as_author(self, setup):
                fixture = setup

                # robot author
                create_commit_sequence_in_project(
                    fixture.organization,
                    fixture.project,
                    fixture.project.repositories[0],
                    fixture.contributor_a,
                    end_date=datetime.utcnow(),
                    start_date_offset_days=60,
                    days_increment=1,
                    commits_common=fixture.commits_common,
                    author=fixture.contributor_c_robot
                )
                # no robot author
                create_commit_sequence_in_project(
                    fixture.organization,
                    fixture.project,
                    fixture.project.repositories[1],
                    fixture.contributor_b,
                    end_date=datetime.utcnow(),
                    start_date_offset_days=60,
                    days_increment=1,
                    commits_common=fixture.commits_common,
                )

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=30,
                    window=30,
                    sample=7,

                ))
                assert result['data']
                project = result['data']['project']
                assert len(project['contributorDetail']) == 5
