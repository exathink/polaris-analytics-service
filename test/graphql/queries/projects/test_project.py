# -*- coding: utf-8 -*-
# Copyright: © Exathink, LLC (2011-2020) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from graphene.test import Client
from datetime import timedelta
from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *
from test.graphql.queries.projects.shared_fixtures import exclude_repos_from_project
from polaris.analytics.db.enums import WorkItemsStateType
from test.fixtures.graphql import WorkItemImportApiHelper
from polaris.common.enums import JiraWorkItemType
from polaris.utils.collections import dict_select, dict_to_object


@pytest.fixture
def projects_import_commits_fixture(org_repo_fixture, cleanup):
    _, projects, repositories = org_repo_fixture

    commit_common_fields = dict(
        commit_date_tz_offset=0,
        committer_alias_key=test_contributor_key,
        author_date=datetime.utcnow(),
        author_date_tz_offset=0,
        author_alias_key=test_contributor_key,
        created_at=datetime.utcnow(),
        commit_message='a change'

    )

    api.import_new_commits(
        organization_key=test_organization_key,
        repository_key=repositories['alpha'].key,
        new_commits=[
            dict(
                source_commit_id='a-XXXX',
                commit_date="11/1/2019",
                key=uuid.uuid4().hex,
                **commit_common_fields
            ),
            dict(
                source_commit_id='a-YYYY',
                commit_date="11/2/2019",
                key=uuid.uuid4().hex,
                **commit_common_fields
            )
        ],
        new_contributors=[
            dict(
                name='Joe Blow',
                key=test_contributor_key,
                alias='joe@blow.com'
            )
        ]
    )

    api.import_new_commits(
        organization_key=test_organization_key,
        repository_key=repositories['gamma'].key,
        new_commits=[
            dict(
                source_commit_id='b-XXXX',
                commit_date="10/1/2019",
                key=uuid.uuid4().hex,
                **commit_common_fields
            ),
            dict(
                source_commit_id='b-YYYY',
                commit_date="11/1/2019",
                key=uuid.uuid4().hex,
                **commit_common_fields
            )
        ],
        new_contributors=[
            dict(
                name='Joe Blow',
                key=test_contributor_key,
                alias='joe@blow.com'
            )
        ]
    )
    yield projects, repositories


class TestProjectRepositories:
    def it_returns_the_repositories_in_the_project(self, projects_import_commits_fixture):
        projects, repositories = projects_import_commits_fixture

        client = Client(schema)
        query = """
                    query getProjectRepositories($project_key:String!) {
                        project(key: $project_key) {
                            repositories {
                                edges {
                                    node {
                                        id
                                        name
                                    }
                                }
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(project_key=projects['mercury'].key))
        assert 'data' in result
        assert {edge['node']['name'] for edge in result['data']['project']['repositories']['edges']} == {
            'alpha',
            'beta'
        }

    def it_shows_the_excluded_attribute(self, projects_import_commits_fixture):
        projects, repositories = projects_import_commits_fixture

        client = Client(schema)
        query = """
                    query getProjectRepositories($project_key:String!) {
                        project(key: $project_key) {
                            repositories (interfaces: [Excluded]) {
                                edges {
                                    node {
                                        id
                                        name
                                        excluded
                                    }
                                }
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(project_key=projects['mercury'].key))
        assert 'data' in result
        assert {
                   (edge['node']['name'], edge['node']['excluded'])
                   for edge in result['data']['project']['repositories']['edges']
               } == {
                   ('alpha', False),
                   ('beta', False)
               }

    def it_hides_the_excluded_repos_by_default(self, projects_import_commits_fixture):
        projects, repositories = projects_import_commits_fixture

        exclude_repos_from_project(projects['mercury'], [repositories['alpha']])

        client = Client(schema)
        query = """
                    query getProjectRepositories($project_key:String!) {
                        project(key: $project_key) {
                            repositories (interfaces: [Excluded]) {
                                edges {
                                    node {
                                        id
                                        name
                                        excluded
                                    }
                                }
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(project_key=projects['mercury'].key))
        assert 'data' in result
        assert {
                   (edge['node']['name'], edge['node']['excluded'])
                   for edge in result['data']['project']['repositories']['edges']
               } == {

                   ('beta', False)
               }

    def it_shows_the_excluded_repos_when_show_excluded_is_true(self, projects_import_commits_fixture):
        projects, repositories = projects_import_commits_fixture

        exclude_repos_from_project(projects['mercury'], [repositories['alpha']])

        client = Client(schema)
        query = """
                    query getProjectRepositories($project_key:String!) {
                        project(key: $project_key) {
                            repositories (interfaces: [Excluded], showExcluded: true) {
                                edges {
                                    node {
                                        id
                                        name
                                        excluded
                                    }
                                }
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(project_key=projects['mercury'].key))
        assert 'data' in result
        assert {
                   (edge['node']['name'], edge['node']['excluded'])
                   for edge in result['data']['project']['repositories']['edges']
               } == {
                ('alpha', True),
                ('beta', False)
               }


class TestProjectContributorCount:

    def it_implements_the_contributor_count_interface(self, project_fixture):
        _, _, _, test_project = project_fixture

        client = Client(schema)
        query = """
            query getProjectWorkItems($project_key:String!) {
                project(key: $project_key, interfaces: [ContributorCount]) {
                    contributorCount
                }
            }
        """
        result = client.execute(query, variable_values=dict(project_key=test_project.key))
        assert 'data' in result
        project = result['data']['project']
        assert project['contributorCount'] == 1


    def it_excludes_contributions_from_excluded_repos(self, project_fixture):
        _, _, _, test_project = project_fixture

        exclude_repos_from_project(test_project, test_project.repositories)
        client = Client(schema)
        query = """
            query getProjectWorkItems($project_key:String!) {
                project(key: $project_key, interfaces: [ContributorCount]) {
                    contributorCount
                }
            }
        """
        result = client.execute(query, variable_values=dict(project_key=test_project.key))
        assert 'data' in result
        project = result['data']['project']
        assert project['contributorCount'] == 0


    def it_returns_contributor_counts_for_a_specified_days_interval(self, project_fixture):
        _, _, _, test_project = project_fixture

        client = Client(schema)
        query = """
            query getProjectWorkItems($project_key:String!, $days: Int!) {
                project(key: $project_key, interfaces: [ContributorCount], contributorCountDays: $days) {
                    contributorCount
                }
            }
        """
        result = client.execute(query, variable_values=dict(
            project_key=test_project.key,
            days=7
        ))
        assert 'data' in result
        project = result['data']['project']
        assert project['contributorCount'] == 1

    def it_returns_zero_as_contributor_counts_for_a_specified_days_interval_if_there_are_no_contributors(
            self, projects_import_commits_fixture):
        projects, _ = projects_import_commits_fixture

        client = Client(schema)
        query = """
            query getProjectWorkItems($project_key:String!, $days: Int!) {
                project(key: $project_key, interfaces: [ContributorCount], contributorCountDays: $days) {
                    contributorCount
                }
            }
        """
        result = client.execute(query, variable_values=dict(
            project_key=projects['mercury'].key,
            days=7
        ))
        assert 'data' in result
        project = result['data']['project']
        assert project['contributorCount'] == 0


class TestProjectDeliveryCycleSpan:

    def it_returns_the_delivery_cycle_span(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )
            for i in range(0, 3)
        ]

        api_helper.import_work_items(work_items)

        api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=1))])
        api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=2))])
        api_helper.update_work_items([(2, 'closed', start_date + timedelta(days=4))])

        client = Client(schema)
        query = """
            query getProjectDeliveryCycleSpan($project_key: String!) {
                project(key: $project_key, interfaces:[DeliveryCycleSpan]) {
                    earliestClosedDate
                    latestClosedDate
                }
            }
        """
        result = client.execute(query, variable_values=dict(project_key=project.key))
        assert result['data']
        project = result['data']['project']

        assert (graphql_date(project['earliestClosedDate']) - start_date).days == 1
        assert (graphql_date(project['latestClosedDate']) - start_date).days == 4

    def it_respects_the_specs_only_parameter(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )
            for i in range(0, 3)
        ]

        api_helper.import_work_items(work_items)

        api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=1))])
        api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=2))])
        api_helper.update_work_items([(2, 'closed', start_date + timedelta(days=4))])

        api_helper.update_delivery_cycles(([(0, dict(property='commit_count', value=1))]))
        api_helper.update_delivery_cycles(([(1, dict(property='commit_count', value=1))]))

        client = Client(schema)
        query = """
            query getProjectDeliveryCycleSpan($project_key: String!) {
                project(key: $project_key, interfaces:[DeliveryCycleSpan], specsOnly: true) {
                    earliestClosedDate
                    latestClosedDate
                }
            }
        """
        result = client.execute(query, variable_values=dict(project_key=project.key))
        assert result['data']
        project = result['data']['project']

        assert (graphql_date(project['earliestClosedDate']) - start_date).days == 1
        assert (graphql_date(project['latestClosedDate']) - start_date).days == 2


class TestProjectAggregateCycleMetrics:

    def it_return_correct_results_when_there_are_no_closed_items(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )
            for i in range(0, 3)
        ]

        api_helper.import_work_items(work_items)

        api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(0, 'done', start_date + timedelta(days=4))])

        client = Client(schema)
        query = """
                query getProjectCycleMetrics($project_key:String!, $days: Int, $percentile: Float) {
                    project(key: $project_key, interfaces: [AggregateCycleMetrics], 
                            closedWithinDays: $days, cycleMetricsTargetPercentile: $percentile
                    ) {
                        ... on AggregateCycleMetrics {
                            measurementDate
                            measurementWindow
                            minLeadTime
                            avgLeadTime
                            maxLeadTime
                            minCycleTime
                            avgCycleTime
                            maxCycleTime
                            percentileLeadTime
                            percentileCycleTime
                            targetPercentile
                            earliestClosedDate
                            latestClosedDate
                            workItemsInScope
                            workItemsWithNullCycleTime

            }
        }
    }
    """
        result = client.execute(query, variable_values=dict(project_key=project.key, days=30, percentile=0.70))
        assert result['data']
        project = result['data']['project']
        assert project['measurementDate']
        assert project['measurementWindow']
        assert not project['minLeadTime']
        assert not project['avgLeadTime']
        assert not project['maxLeadTime']
        assert not project['minCycleTime']
        assert not project['avgCycleTime']
        assert not project['maxCycleTime']
        assert not project['percentileLeadTime']
        assert not project['percentileCycleTime']
        assert project['targetPercentile'] == 0.7
        assert project['workItemsInScope'] == 0
        assert project['workItemsWithNullCycleTime'] == 0

    def it_computes_cycle_time_metrics_when_there_is_exactly_one_closed_item(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )
            for i in range(0, 3)
        ]

        api_helper.import_work_items(work_items)

        api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(0, 'done', start_date + timedelta(days=4))])
        api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=6))])

        client = Client(schema)
        query = """
                    query getProjectCycleMetrics($project_key:String!, $days: Int, $percentile: Float) {
                        project(key: $project_key, interfaces: [AggregateCycleMetrics], 
                                closedWithinDays: $days, cycleMetricsTargetPercentile: $percentile
                        ) {
                            ... on AggregateCycleMetrics {
                                minLeadTime
                                avgLeadTime
                                maxLeadTime
                                minCycleTime
                                avgCycleTime
                                maxCycleTime
                                percentileLeadTime
                                percentileCycleTime
                                targetPercentile
                                earliestClosedDate
                                latestClosedDate
                                workItemsInScope
                                workItemsWithNullCycleTime
                            
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(project_key=project.key, days=30, percentile=0.70))

        assert result['data']
        project = result['data']['project']
        assert project['minLeadTime'] == 6.0
        assert project['avgLeadTime'] == 6.0
        assert project['maxLeadTime'] == 6.0
        assert project['minCycleTime'] == 5.0
        assert project['avgCycleTime'] == 5.0
        assert project['maxCycleTime'] == 5.0
        assert project['percentileLeadTime'] == 6.0
        assert project['percentileCycleTime'] == 5.0
        assert project['targetPercentile'] == 0.7
        assert project['workItemsInScope'] == 1
        assert project['workItemsWithNullCycleTime'] == 0
        assert (graphql_date(project['earliestClosedDate']) - start_date).days == 6
        assert (graphql_date(project['latestClosedDate']) - start_date).days == 6

    def it_computes_cycle_time_metrics_when_there_are_two_closed_items(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )
            for i in range(0, 3)
        ]

        api_helper.import_work_items(work_items)

        api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(0, 'done', start_date + timedelta(days=4))])
        api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=6))])

        api_helper.update_work_items([(1, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(1, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(1, 'done', start_date + timedelta(days=6))])
        api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=8))])

        client = Client(schema)
        query = """
                    query getProjectCycleMetrics($project_key:String!, $days: Int, $percentile: Float) {
                        project(key: $project_key, interfaces: [AggregateCycleMetrics], 
                                closedWithinDays: $days, cycleMetricsTargetPercentile: $percentile
                        ) {
                            ... on AggregateCycleMetrics {
                                minLeadTime
                                avgLeadTime
                                maxLeadTime
                                minCycleTime
                                avgCycleTime
                                maxCycleTime
                                percentileLeadTime
                                percentileCycleTime
                                targetPercentile
                                earliestClosedDate
                                latestClosedDate
                                workItemsInScope
                                workItemsWithNullCycleTime

                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(project_key=project.key, days=30, percentile=0.70))

        assert result['data']
        project = result['data']['project']
        assert project['minLeadTime'] == 6.0
        assert project['avgLeadTime'] == 7.0
        assert project['maxLeadTime'] == 8.0
        assert project['minCycleTime'] == 5.0
        assert project['avgCycleTime'] == 6.0
        assert project['maxCycleTime'] == 7.0
        assert project['percentileLeadTime'] == 8.0
        assert project['percentileCycleTime'] == 7.0
        assert project['targetPercentile'] == 0.7
        assert project['workItemsInScope'] == 2
        assert project['workItemsWithNullCycleTime'] == 0
        assert (graphql_date(project['earliestClosedDate']) - start_date).days == 6
        assert (graphql_date(project['latestClosedDate']) - start_date).days == 8

    def it_computes_cycle_time_metrics_when_there_are_reopened_work_items(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )
            for i in range(0, 3)
        ]

        api_helper.import_work_items(work_items)

        api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(0, 'done', start_date + timedelta(days=4))])
        api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=6))])

        api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=7))])
        api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=8))])
        api_helper.update_work_items([(0, 'done', start_date + timedelta(days=9))])
        api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=10))])

        client = Client(schema)
        query = """
                    query getProjectCycleMetrics($project_key:String!, $days: Int, $percentile: Float) {
                        project(key: $project_key, interfaces: [AggregateCycleMetrics], 
                                closedWithinDays: $days, cycleMetricsTargetPercentile: $percentile
                        ) {
                            ... on AggregateCycleMetrics {
                                minLeadTime
                                avgLeadTime
                                maxLeadTime
                                minCycleTime
                                avgCycleTime
                                maxCycleTime
                                percentileLeadTime
                                percentileCycleTime
                                targetPercentile
                                earliestClosedDate
                                latestClosedDate
                                workItemsInScope
                                workItemsWithNullCycleTime

                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(project_key=project.key, days=30, percentile=0.70))

        assert result['data']
        project = result['data']['project']
        assert project['minLeadTime'] == 3.0
        assert project['avgLeadTime'] == 4.5
        assert project['maxLeadTime'] == 6.0
        assert project['minCycleTime'] == 3.0
        assert project['avgCycleTime'] == 4.0
        assert project['maxCycleTime'] == 5.0
        assert project['percentileLeadTime'] == 6.0
        assert project['percentileCycleTime'] == 5.0
        assert project['targetPercentile'] == 0.7
        # re-opened items only count as a single work items for throughput purposes
        assert project['workItemsInScope'] == 1
        assert project['workItemsWithNullCycleTime'] == 0
        assert (graphql_date(project['earliestClosedDate']) - start_date).days == 6
        assert (graphql_date(project['latestClosedDate']) - start_date).days == 10

    def it_computes_cycle_time_metrics_when_there_are_three_closed_items(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )
            for i in range(0, 3)
        ]

        api_helper.import_work_items(work_items)

        api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(0, 'done', start_date + timedelta(days=4))])
        api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=6))])

        api_helper.update_work_items([(1, 'upnext', start_date + timedelta(days=2))])
        api_helper.update_work_items([(1, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(1, 'done', start_date + timedelta(days=6))])
        api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=8))])

        api_helper.update_work_items([(2, 'upnext', start_date + timedelta(days=3))])
        api_helper.update_work_items([(2, 'doing', start_date + timedelta(days=4))])
        api_helper.update_work_items([(2, 'done', start_date + timedelta(days=5))])
        api_helper.update_work_items([(2, 'closed', start_date + timedelta(days=10))])

        client = Client(schema)
        query = """
                    query getProjectCycleMetrics($project_key:String!, $days: Int, $percentile: Float) {
                        project(key: $project_key, interfaces: [AggregateCycleMetrics], 
                                closedWithinDays: $days, cycleMetricsTargetPercentile: $percentile
                        ) {
                            ... on AggregateCycleMetrics {
                                minLeadTime
                                avgLeadTime
                                maxLeadTime
                                minCycleTime
                                avgCycleTime
                                maxCycleTime
                                percentileLeadTime
                                percentileCycleTime
                                targetPercentile
                                earliestClosedDate
                                latestClosedDate
                                workItemsInScope
                                workItemsWithNullCycleTime

                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(project_key=project.key, days=30, percentile=0.70))

        assert result['data']
        project = result['data']['project']
        assert project['minLeadTime'] == 6.0
        assert project['avgLeadTime'] == 8.0
        assert project['maxLeadTime'] == 10.0
        assert project['minCycleTime'] == 5.0
        assert project['avgCycleTime'] == 6.0
        assert project['maxCycleTime'] == 7.0
        assert project['percentileLeadTime'] == 10.0
        assert project['percentileCycleTime'] == 7.0
        assert project['targetPercentile'] == 0.7
        assert project['workItemsInScope'] == 3
        assert project['workItemsWithNullCycleTime'] == 0
        assert (graphql_date(project['earliestClosedDate']) - start_date).days == 6
        assert (graphql_date(project['latestClosedDate']) - start_date).days == 10

    def it_respects_target_percentile(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )
            for i in range(0, 3)
        ]

        api_helper.import_work_items(work_items)

        api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(0, 'done', start_date + timedelta(days=4))])
        api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=6))])

        api_helper.update_work_items([(1, 'upnext', start_date + timedelta(days=2))])
        api_helper.update_work_items([(1, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(1, 'done', start_date + timedelta(days=6))])
        api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=8))])

        api_helper.update_work_items([(2, 'upnext', start_date + timedelta(days=3))])
        api_helper.update_work_items([(2, 'doing', start_date + timedelta(days=4))])
        api_helper.update_work_items([(2, 'done', start_date + timedelta(days=5))])
        api_helper.update_work_items([(2, 'closed', start_date + timedelta(days=10))])

        client = Client(schema)
        query = """
                    query getProjectCycleMetrics($project_key:String!, $days: Int, $percentile: Float) {
                        project(key: $project_key, interfaces: [AggregateCycleMetrics], 
                                closedWithinDays: $days, cycleMetricsTargetPercentile: $percentile
                        ) {
                            ... on AggregateCycleMetrics {
                                
                                percentileLeadTime
                                percentileCycleTime
                                targetPercentile
                                

                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(project_key=project.key, days=30, percentile=0.50))

        assert result['data']
        project = result['data']['project']
        assert project['percentileLeadTime'] == 8.0
        assert project['percentileCycleTime'] == 6.0
        assert project['targetPercentile'] == 0.5

    def it_computes_work_items_with_null_cycle_times(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )
            for i in range(0, 3)
        ]

        api_helper.import_work_items(work_items)

        api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(0, 'done', start_date + timedelta(days=4))])
        api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=6))])

        api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=8))])

        api_helper.update_work_items([(2, 'upnext', start_date + timedelta(days=3))])
        api_helper.update_work_items([(2, 'doing', start_date + timedelta(days=4))])
        api_helper.update_work_items([(2, 'done', start_date + timedelta(days=5))])
        api_helper.update_work_items([(2, 'closed', start_date + timedelta(days=10))])

        client = Client(schema)
        query = """
                    query getProjectCycleMetrics($project_key:String!, $days: Int, $percentile: Float) {
                        project(key: $project_key, interfaces: [AggregateCycleMetrics], 
                                closedWithinDays: $days, cycleMetricsTargetPercentile: $percentile
                        ) {
                            ... on AggregateCycleMetrics {
                                avgCycleTime
                                workItemsWithNullCycleTime


                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(project_key=project.key, days=30, percentile=0.50))

        assert result['data']
        project = result['data']['project']
        assert project['workItemsWithNullCycleTime'] == 1

    def it_respects_the_closed_within_days_parameter(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )
            for i in range(0, 3)
        ]

        api_helper.import_work_items(work_items)

        api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(0, 'done', start_date + timedelta(days=4))])
        api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=6))])

        api_helper.update_work_items([(1, 'upnext', start_date + timedelta(days=2))])
        api_helper.update_work_items([(1, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(1, 'done', start_date + timedelta(days=6))])
        api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=8))])

        api_helper.update_work_items([(2, 'upnext', start_date + timedelta(days=3))])
        api_helper.update_work_items([(2, 'doing', start_date + timedelta(days=4))])
        api_helper.update_work_items([(2, 'done', start_date + timedelta(days=5))])
        api_helper.update_work_items([(2, 'closed', start_date + timedelta(days=10))])

        client = Client(schema)
        query = """
                    query getProjectCycleMetrics($project_key:String!, $days: Int, $percentile: Float) {
                        project(key: $project_key, interfaces: [AggregateCycleMetrics], 
                                closedWithinDays: $days, cycleMetricsTargetPercentile: $percentile
                        ) {
                            ... on AggregateCycleMetrics {
                                minLeadTime
                                avgLeadTime
                                maxLeadTime
                                minCycleTime
                                avgCycleTime
                                maxCycleTime
                                percentileLeadTime
                                percentileCycleTime
                                targetPercentile
                                earliestClosedDate
                                latestClosedDate
                                workItemsInScope
                                workItemsWithNullCycleTime

                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(project_key=project.key, days=1, percentile=0.70))

        assert result['data']
        project = result['data']['project']
        assert project['minLeadTime'] == 10.0
        assert project['avgLeadTime'] == 10.0
        assert project['maxLeadTime'] == 10.0
        assert project['minCycleTime'] == 7.0
        assert project['avgCycleTime'] == 7.0
        assert project['maxCycleTime'] == 7.0
        assert project['percentileLeadTime'] == 10.0
        assert project['percentileCycleTime'] == 7.0
        assert project['targetPercentile'] == 0.7
        assert project['workItemsInScope'] == 1
        assert project['workItemsWithNullCycleTime'] == 0
        assert (graphql_date(project['earliestClosedDate']) - start_date).days == 10
        assert (graphql_date(project['latestClosedDate']) - start_date).days == 10

    def it_respects_the_defects_only_parameter(self, api_work_items_import_fixture):
        organization, project, work_items_source, _ = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items_common = dict(
            work_item_type='issue',
            url='http://foo.com',
            tags=['ares2'],
            description='foo',
            source_id=str(uuid.uuid4()),
            is_epic=False,
            parent_id=None,
        )

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue 1',
                display_id='1001',
                state='backlog',
                is_bug=True,
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            ),
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue 2',
                display_id='1001',
                state='backlog',
                is_bug=True,
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            ),
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue 2',
                display_id='1002',
                state='backlog',
                is_bug=False,
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )

        ]

        api_helper.import_work_items(work_items)

        api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(0, 'done', start_date + timedelta(days=4))])
        api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=6))])

        api_helper.update_work_items([(1, 'upnext', start_date + timedelta(days=2))])
        api_helper.update_work_items([(1, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(1, 'done', start_date + timedelta(days=6))])
        api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=8))])

        api_helper.update_work_items([(2, 'upnext', start_date + timedelta(days=3))])
        api_helper.update_work_items([(2, 'doing', start_date + timedelta(days=4))])
        api_helper.update_work_items([(2, 'done', start_date + timedelta(days=5))])
        api_helper.update_work_items([(2, 'closed', start_date + timedelta(days=10))])

        client = Client(schema)
        query = """
                    query getProjectCycleMetrics($project_key:String!, $days: Int, $percentile: Float) {
                        project(key: $project_key, interfaces: [AggregateCycleMetrics], 
                                closedWithinDays: $days, cycleMetricsTargetPercentile: $percentile,
                                defectsOnly: true
                        ) {
                            ... on AggregateCycleMetrics {
                                minLeadTime
                                avgLeadTime
                                maxLeadTime
                                minCycleTime
                                avgCycleTime
                                maxCycleTime
                                percentileLeadTime
                                percentileCycleTime
                                targetPercentile
                                earliestClosedDate
                                latestClosedDate
                                workItemsInScope
                                workItemsWithNullCycleTime

                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(project_key=project.key, days=30, percentile=0.70))

        assert result['data']
        project = result['data']['project']
        assert project['minLeadTime'] == 6.0
        assert project['avgLeadTime'] == 7.0
        assert project['maxLeadTime'] == 8.0
        assert project['minCycleTime'] == 5.0
        assert project['avgCycleTime'] == 5.5
        assert project['maxCycleTime'] == 6.0
        assert project['percentileLeadTime'] == 8.0
        assert project['percentileCycleTime'] == 6.0
        assert project['targetPercentile'] == 0.7
        assert project['workItemsInScope'] == 2
        assert project['workItemsWithNullCycleTime'] == 0
        assert (graphql_date(project['earliestClosedDate']) - start_date).days == 6
        assert (graphql_date(project['latestClosedDate']) - start_date).days == 8

    def it_respects_the_specs_only_parameter(self, api_work_items_import_fixture):
        organization, project, work_items_source, _ = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items_common = dict(
            work_item_type='issue',
            url='http://foo.com',
            tags=['ares2'],
            description='foo',
            source_id=str(uuid.uuid4()),
            is_bug=False,
            is_epic=False,
        )

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue 1',
                display_id='1001',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            ),
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue 2',
                display_id='1001',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            ),
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue 2',
                display_id='1002',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )

        ]

        api_helper.import_work_items(work_items)

        api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(0, 'done', start_date + timedelta(days=4))])
        api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=6))])
        # make item 0 a spec
        api_helper.update_delivery_cycles(([(0, dict(property='commit_count', value=2))]))

        api_helper.update_work_items([(1, 'upnext', start_date + timedelta(days=2))])
        api_helper.update_work_items([(1, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(1, 'done', start_date + timedelta(days=6))])
        api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=8))])
        # make item 1 a spec
        api_helper.update_delivery_cycles(([(1, dict(property='commit_count', value=2))]))

        api_helper.update_work_items([(2, 'upnext', start_date + timedelta(days=3))])
        api_helper.update_work_items([(2, 'doing', start_date + timedelta(days=4))])
        api_helper.update_work_items([(2, 'done', start_date + timedelta(days=5))])
        api_helper.update_work_items([(2, 'closed', start_date + timedelta(days=10))])

        client = Client(schema)
        query = """
                    query getProjectCycleMetrics($project_key:String!, $days: Int, $percentile: Float) {
                        project(key: $project_key, interfaces: [AggregateCycleMetrics], 
                                closedWithinDays: $days, cycleMetricsTargetPercentile: $percentile,
                                specsOnly: true
                        ) {
                            ... on AggregateCycleMetrics {
                                minLeadTime
                                avgLeadTime
                                maxLeadTime
                                minCycleTime
                                avgCycleTime
                                maxCycleTime
                                percentileLeadTime
                                percentileCycleTime
                                targetPercentile
                                earliestClosedDate
                                latestClosedDate
                                workItemsInScope
                                workItemsWithCommits
                                workItemsWithNullCycleTime

                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(project_key=project.key, days=30, percentile=0.70))

        assert result['data']
        project = result['data']['project']
        assert project['minLeadTime'] == 6.0
        assert project['avgLeadTime'] == 7.0
        assert project['maxLeadTime'] == 8.0
        assert project['minCycleTime'] == 5.0
        assert project['avgCycleTime'] == 5.5
        assert project['maxCycleTime'] == 6.0
        assert project['percentileLeadTime'] == 8.0
        assert project['percentileCycleTime'] == 6.0
        assert project['targetPercentile'] == 0.7
        assert project['workItemsInScope'] == 2
        assert project['workItemsWithNullCycleTime'] == 0
        assert (graphql_date(project['earliestClosedDate']) - start_date).days == 6
        assert (graphql_date(project['latestClosedDate']) - start_date).days == 8

    def it_excludes_jira_epics_from_cycle_metrics_calculations(self, api_work_items_import_fixture):
        organization, project, work_items_source, _ = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items_common = dict(
            is_bug=True,
            url='http://foo.com',
            tags=['ares2'],
            description='foo',
            source_id=str(uuid.uuid4()),
            is_epic=False,
            parent_id=None,
        )

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name='story 1',
                work_item_type=JiraWorkItemType.story.value,
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            ),
            dict(
                key=uuid.uuid4().hex,
                name='subtask 1',
                work_item_type=JiraWorkItemType.sub_task.value,
                display_id='1001',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            ),
            dict(
                key=uuid.uuid4().hex,
                name='epic 1',
                work_item_type=JiraWorkItemType.epic.value,
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )

        ]

        api_helper.import_work_items(work_items)

        api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(0, 'done', start_date + timedelta(days=4))])
        api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=6))])

        api_helper.update_work_items([(1, 'upnext', start_date + timedelta(days=2))])
        api_helper.update_work_items([(1, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(1, 'done', start_date + timedelta(days=6))])
        api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=8))])

        api_helper.update_work_items([(2, 'upnext', start_date + timedelta(days=3))])
        api_helper.update_work_items([(2, 'doing', start_date + timedelta(days=4))])
        api_helper.update_work_items([(2, 'done', start_date + timedelta(days=5))])
        api_helper.update_work_items([(2, 'closed', start_date + timedelta(days=10))])

        client = Client(schema)
        query = """
                    query getProjectCycleMetrics($project_key:String!, $days: Int, $percentile: Float) {
                        project(key: $project_key, interfaces: [AggregateCycleMetrics], 
                                closedWithinDays: $days, cycleMetricsTargetPercentile: $percentile
                        ) {
                            ... on AggregateCycleMetrics {
                                minLeadTime
                                avgLeadTime
                                maxLeadTime
                                minCycleTime
                                avgCycleTime
                                maxCycleTime
                                percentileLeadTime
                                percentileCycleTime
                                targetPercentile
                                earliestClosedDate
                                latestClosedDate
                                workItemsInScope
                                workItemsWithNullCycleTime

                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(project_key=project.key, days=30, percentile=0.70))

        assert result['data']
        project = result['data']['project']
        assert project['minLeadTime'] == 6.0
        assert project['avgLeadTime'] == 7.0
        assert project['maxLeadTime'] == 8.0
        assert project['minCycleTime'] == 5.0
        assert project['avgCycleTime'] == 5.5
        assert project['maxCycleTime'] == 6.0
        assert project['percentileLeadTime'] == 8.0
        assert project['percentileCycleTime'] == 6.0
        assert project['targetPercentile'] == 0.7
        assert project['workItemsInScope'] == 2
        assert project['workItemsWithNullCycleTime'] == 0
        assert (graphql_date(project['earliestClosedDate']) - start_date).days == 6
        assert (graphql_date(project['latestClosedDate']) - start_date).days == 8

class TestProjectWorkItemDeliveryCycles(WorkItemApiImportTest):

    class TestWorkItemDeliveryCycles:
        def it_implements_the_named_node_interface(self, setup):
            fixture = setup

            project = fixture.project
            api_helper = fixture.api_helper
            work_items_common_fields = fixture.work_items_common

            start_date = datetime.utcnow() - timedelta(days=10)

            work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name=f'Issue {i}',
                    display_id='1000',
                    state='backlog',
                    created_at=start_date,
                    updated_at=start_date,
                    **work_items_common_fields
                )
                for i in range(0, 3)
            ]

            api_helper.import_work_items(work_items)
            client = Client(schema)
            query = """
                                    query getProjectDeliveryCycles($project_key:String!) {
                                        project(key: $project_key) {
                                            workItemDeliveryCycles {
                                                edges {
                                                    node {
                                                        id
                                                        name
                                                        key
                                                    }
                                                }
                                            }
                                        }
                                    }
                                """
            result = client.execute(query, variable_values=dict(project_key=project.key))
            assert result['data']
            nodes = [edge['node'] for edge in result['data']['project']['workItemDeliveryCycles']['edges']]
            assert len(nodes) == 3
            for node in nodes:
                assert node['id']
                assert node['name']
                assert node['key']

        def it_implements_the_work_item_info_interface(self, setup):
            fixture = setup
            project = fixture.project
            work_items_common_fields = fixture.work_items_common
            api_helper= fixture.api_helper


            start_date = datetime.utcnow() - timedelta(days=10)

            work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name=f'Issue {i}',
                    display_id='1000',
                    state='backlog',
                    created_at=start_date,
                    updated_at=start_date,
                    **work_items_common_fields
                )
                for i in range(0, 3)
            ]

            api_helper.import_work_items(work_items)
            client = Client(schema)
            query = """
                                    query getProjectDeliveryCycles($project_key:String!) {
                                        project(key: $project_key) {
                                            workItemDeliveryCycles {
                                                edges {
                                                    node {
                                                        workItemType
                                                        displayId
                                                        url
                                                        description
                                                        state
                                                        createdAt
                                                        updatedAt
                                                        stateType
                                                    }
                                                }
                                            }
                                        }
                                    }
                                """
            result = client.execute(query, variable_values=dict(project_key=project.key))
            assert result['data']
            nodes = [edge['node'] for edge in result['data']['project']['workItemDeliveryCycles']['edges']]
            assert len(nodes) == 3
            for node in nodes:
                assert node['workItemType']
                assert node['displayId']
                assert node['url']
                assert node['description']
                assert node['state']
                assert node['stateType']
                assert node['createdAt']
                assert node['updatedAt']

        def it_implements_the_work_items_source_ref_interface(self, setup):
            fixture = setup

            project = fixture.project
            api_helper = fixture.api_helper
            work_items_common_fields = fixture.work_items_common

            start_date = datetime.utcnow() - timedelta(days=10)

            work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name=f'Issue {i}',
                    display_id='1000',
                    state='backlog',
                    created_at=start_date,
                    updated_at=start_date,
                    **work_items_common_fields
                )
                for i in range(0, 3)
            ]

            api_helper.import_work_items(work_items)
            client = Client(schema)
            query = """
                                    query getProjectDeliveryCycles($project_key:String!) {
                                        project(key: $project_key) {
                                            workItemDeliveryCycles {
                                                edges {
                                                    node {
                                                        workItemsSourceName
                                                        workItemsSourceKey
                                                        workTrackingIntegrationType
                                                    }
                                                }
                                            }
                                        }
                                    }
                                """
            result = client.execute(query, variable_values=dict(project_key=project.key))
            assert result['data']
            nodes = [edge['node'] for edge in result['data']['project']['workItemDeliveryCycles']['edges']]
            assert len(nodes) == 3
            for node in nodes:
                assert node['workItemsSourceName']
                assert node['workItemsSourceKey']
                assert node['workTrackingIntegrationType']


        def it_implements_the_delivery_cycle_info_interface(self, setup):
            fixture = setup

            project = fixture.project
            api_helper = fixture.api_helper
            work_items_common_fields = fixture.work_items_common

            start_date = datetime.utcnow() - timedelta(days=10)

            work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name=f'Issue {i}',
                    display_id='1000',
                    state='backlog',
                    created_at=start_date,
                    updated_at=start_date,
                    **work_items_common_fields
                )
                for i in range(0, 3)
            ]

            api_helper.import_work_items(work_items)
            client = Client(schema)
            query = """
                                    query getProjectDeliveryCycles($project_key:String!) {
                                        project(key: $project_key) {
                                            workItemDeliveryCycles {
                                                edges {
                                                    node {
                                                        closed
                                                        startDate
                                                        endDate
                                                    }
                                                }
                                            }
                                        }
                                    }
                                """
            result = client.execute(query, variable_values=dict(project_key=project.key))
            assert result['data']
            nodes = [edge['node'] for edge in result['data']['project']['workItemDeliveryCycles']['edges']]
            assert len(nodes) == 3
            for node in nodes:
                assert not node['closed']
                assert graphql_date(node['startDate']) == start_date
                assert not node['endDate']

        def it_respects_the_closed_within_days_parameter(self, setup):
            fixture = setup

            project = fixture.project
            api_helper = fixture.api_helper
            work_items_common_fields = fixture.work_items_common

            start_date = datetime.utcnow() - timedelta(days=10)

            work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name=f'Issue {i}',
                    display_id='1000',
                    state='backlog',
                    created_at=start_date,
                    updated_at=start_date,
                    **work_items_common_fields
                )
                for i in range(0, 3)
            ]

            api_helper.import_work_items(work_items)
            api_helper.update_work_items([(1, 'doing', start_date + timedelta(days=2))])
            api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=3))])

            api_helper.update_work_items([(2, 'doing', start_date + timedelta(days=5))])
            api_helper.update_work_items([(2, 'closed', start_date + timedelta(days=8))])

            client = Client(schema)
            query = """
                    query getProjectDeliveryCycles($project_key:String!, $days: Int!) {
                        project(key: $project_key) {
                            workItemDeliveryCycles(closedWithinDays: $days) {
                                edges {
                                    node {
                                        closed
                                        startDate
                                        endDate
                                    }
                                }
                            }
                        }
                    }
                    """
            result = client.execute(query, variable_values=dict(project_key=project.key, days=5))
            assert result['data']
            nodes = [edge['node'] for edge in result['data']['project']['workItemDeliveryCycles']['edges']]
            assert len(nodes) == 1
            assert nodes[0]['closed']
            assert nodes[0]['endDate']

        def it_respects_the_defects_only_parameter(self, setup):
            fixture = setup

            project = fixture.project
            api_helper = fixture.api_helper


            start_date = datetime.utcnow() - timedelta(days=10)

            work_items_common_fields = dict(
                work_item_type='issue',
                url='http://foo.com',
                tags=['ares2'],
                description='foo',
                source_id=str(uuid.uuid4()),
                is_epic=False,
                parent_id=None,
            )

            work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name=f'Issue 1',
                    display_id='1000',
                    is_bug=False,
                    state='backlog',
                    created_at=start_date,
                    updated_at=start_date,
                    **work_items_common_fields
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name=f'Issue 2',
                    display_id='1000',
                    state='backlog',
                    is_bug=True,
                    created_at=start_date,
                    updated_at=start_date,
                    **work_items_common_fields
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name=f'Issue 3',
                    display_id='1000',
                    is_bug=True,
                    state='backlog',
                    created_at=start_date,
                    updated_at=start_date,
                    **work_items_common_fields
                ),

            ]

            api_helper.import_work_items(work_items)
            client = Client(schema)
            query = """
                                    query getProjectDeliveryCycles($project_key:String!) {
                                        project(key: $project_key) {
                                            workItemDeliveryCycles(defectsOnly: true) {
                                                edges {
                                                    node {
                                                        id
                                                        name
                                                        key
                                                    }
                                                }
                                            }
                                        }
                                    }
                                """
            result = client.execute(query, variable_values=dict(project_key=project.key))
            assert result['data']
            nodes = [edge['node'] for edge in result['data']['project']['workItemDeliveryCycles']['edges']]
            assert len(nodes) == 2
            for node in nodes:
                assert node['id']
                assert node['name']
                assert node['key']


        class TestCycleMetrics:

            def it_returns_no_cycle_metrics_when_there_are_no_closed_items(self, setup):
                fixture = setup

                project = fixture.project
                api_helper = fixture.api_helper
                work_items_common_fields = fixture.work_items_common

                start_date = datetime.utcnow() - timedelta(days=10)

                work_items = [
                    dict(
                        key=uuid.uuid4().hex,
                        name=f'Issue {i}',
                        display_id='1000',
                        state='backlog',
                        created_at=start_date,
                        updated_at=start_date,
                        **work_items_common_fields
                    )
                    for i in range(0, 3)
                ]

                api_helper.import_work_items(work_items)

                api_helper.update_work_items([(1, 'doing', start_date + timedelta(days=8))])

                api_helper.update_work_items([(2, 'upnext', start_date + timedelta(days=3))])
                api_helper.update_work_items([(2, 'doing', start_date + timedelta(days=4))])
                api_helper.update_work_items([(2, 'done', start_date + timedelta(days=5))])

                client = Client(schema)
                query = """
                                        query getProjectDeliveryCycles($project_key:String!) {
                                            project(key: $project_key) {
                                                workItemDeliveryCycles (interfaces: [CycleMetrics]){
                                                    edges {
                                                        node {
                                                            leadTime
                                                            cycleTime
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    """
                result = client.execute(query, variable_values=dict(project_key=project.key))
                assert result['data']
                nodes = [edge['node'] for edge in result['data']['project']['workItemDeliveryCycles']['edges']]
                assert len(nodes) == 3
                for node in nodes:
                    assert not node['leadTime']
                    assert not node['cycleTime']

        def it_returns_cycle_metrics_for_closed_items(self, setup):
            fixture = setup

            project = fixture.project
            api_helper = fixture.api_helper
            work_items_common_fields = fixture.work_items_common

            start_date = datetime.utcnow() - timedelta(days=10)

            work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name=f'Issue {i}',
                    display_id='1000',
                    state='backlog',
                    created_at=start_date,
                    updated_at=start_date,
                    **work_items_common_fields
                )
                for i in range(0, 3)
            ]

            api_helper.import_work_items(work_items)

            api_helper.update_work_items([(1, 'doing', start_date + timedelta(days=2))])
            api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=8))])

            api_helper.update_work_items([(2, 'done', start_date + timedelta(days=5))])

            client = Client(schema)
            query = """
                                    query getProjectDeliveryCycles($project_key:String!) {
                                        project(key: $project_key) {
                                            workItemDeliveryCycles (interfaces: [CycleMetrics]){
                                                edges {
                                                    node {
                                                        name
                                                        leadTime
                                                        cycleTime
                                                        latency
                                                        duration
                                                        endDate
                                                    }
                                                }
                                            }
                                        }
                                    }
                                """
            result = client.execute(query, variable_values=dict(project_key=project.key))
            assert result['data']
            nodes = [edge['node'] for edge in result['data']['project']['workItemDeliveryCycles']['edges']]
            assert len(nodes) == 3
            for node in nodes:
                if node['name'] == 'Issue 1':
                    assert node['leadTime'] == 8.0
                    assert node['cycleTime'] == 6.0
                    assert not node['latency']
                    assert not node['duration']
                else:
                    assert not node['leadTime']
                    assert not node['cycleTime']

        def it_returns_cycle_metrics_for_reopened_items(self, setup):
            fixture = setup

            project = fixture.project
            api_helper = fixture.api_helper
            work_items_common_fields = fixture.work_items_common

            start_date = datetime.utcnow() - timedelta(days=10)

            work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name=f'Issue {i}',
                    display_id='1000',
                    state='backlog',
                    created_at=start_date,
                    updated_at=start_date,
                    **work_items_common_fields
                )
                for i in range(0, 3)
            ]

            api_helper.import_work_items(work_items)

            api_helper.update_work_items([(1, 'doing', start_date + timedelta(days=2))])
            api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=8))])

            api_helper.update_work_items([(1, 'doing', start_date + timedelta(days=10))])
            api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=12))])

            api_helper.update_work_items([(2, 'done', start_date + timedelta(days=5))])

            client = Client(schema)
            query = """
                                    query getProjectDeliveryCycles($project_key:String!) {
                                        project(key: $project_key) {
                                            workItemDeliveryCycles (interfaces: [CycleMetrics]){
                                                edges {
                                                    node {
                                                        name
                                                        leadTime
                                                        cycleTime
                                                        endDate
                                                    }
                                                }
                                            }
                                        }
                                    }
                                """
            result = client.execute(query, variable_values=dict(project_key=project.key))
            assert result['data']
            nodes = [edge['node'] for edge in result['data']['project']['workItemDeliveryCycles']['edges']]
            # expect new delivery cycle for re-opened items
            assert len(nodes) == 4
            assert {(node['leadTime'], node['cycleTime']) for node in nodes if node['name'] == 'Issue 1'} == \
                   {(8.0, 6.0), (2.0, 2.0)}
            assert {(node['leadTime'], node['cycleTime']) for node in nodes if node['name'] != 'Issue 1'} == \
                   {(None, None)}


class TestAggregateVsDetailConsistency:

    def it_returns_aggregate_metrics_that_are_consistent_with_the_detail_delivery_cycle_metrics(self,
                                                                                                api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )
            for i in range(0, 3)
        ]

        api_helper.import_work_items(work_items)

        api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=5))])

        api_helper.update_work_items([(1, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=3))])

        api_helper.update_work_items([(2, 'doing', start_date + timedelta(days=4))])
        api_helper.update_work_items([(2, 'closed', start_date + timedelta(days=8))])

        client = Client(schema)
        query = """
                query getProjectDeliveryCycles($project_key:String!, $days: Int!) {
                    project(key: $project_key, closedWithinDays: $days, interfaces: [AggregateCycleMetrics]) {
                        avgLeadTime
                        avgCycleTime
                        
                        workItemDeliveryCycles (closedWithinDays: $days, interfaces: [CycleMetrics]){
                            edges {
                                node {
                                    name
                                    leadTime
                                    cycleTime
                                    endDate
                                }
                            }
                        }
                    }
                }
            """
        result = client.execute(query, variable_values=dict(project_key=project.key, days=10))
        assert result['data']
        project = result['data']['project']
        assert project['avgLeadTime'] == 5.5
        assert project['avgCycleTime'] == 2.5

        nodes = [edge['node'] for edge in project['workItemDeliveryCycles']['edges']]
        assert len(nodes) == 2
        assert {(node['leadTime'], node['cycleTime']) for node in nodes if node['name'] == 'Issue 2'} == \
               {(8.0, 4.0)}
        assert {(node['leadTime'], node['cycleTime']) for node in nodes if node['name'] == 'Issue 1'} == \
               {(3.0, 1.0)}


@pytest.fixture
def project_work_items_source_state_mapping_fixture(org_repo_fixture):
    organization, projects, _ = org_repo_fixture

    project = projects['mercury']
    with db.orm_session() as session:
        session.add(organization)
        session.add(project)

        work_items_source = WorkItemsSource(
            key=uuid.uuid4(),
            organization_key=organization.key,
            integration_type='jira',
            commit_mapping_scope='repository',
            commit_mapping_scope_key=None,
            project_id=project.id,
            **work_items_source_common
        )
        work_items_source.init_state_map(
            [
                dict(state='created', state_type=WorkItemsStateType.backlog.value),
                dict(state='backlog', state_type=WorkItemsStateType.backlog.value),
                dict(state='upnext', state_type=WorkItemsStateType.open.value),
                dict(state='doing', state_type=WorkItemsStateType.wip.value),
                dict(state='done', state_type=WorkItemsStateType.complete.value),
                dict(state='closed', state_type=WorkItemsStateType.closed.value),
            ]
        )
        organization.work_items_sources.append(work_items_source)

        work_items_source = WorkItemsSource(
            key=uuid.uuid4(),
            organization_key=organization.key,
            integration_type='github',
            commit_mapping_scope='repository',
            commit_mapping_scope_key=None,
            project_id=project.id,
            **work_items_source_common
        )
        work_items_source.init_state_map(
            [
                dict(state='created', state_type=WorkItemsStateType.backlog.value),
                dict(state='open', state_type=WorkItemsStateType.backlog.value),
                dict(state='closed', state_type=WorkItemsStateType.closed.value),
            ]
        )
        organization.work_items_sources.append(work_items_source)

    yield project

    db.connection().execute("delete  from analytics.work_items_source_state_map")
    db.connection().execute("delete  from analytics.work_items_sources")


class TestProjectWorkItemsSourceWorkItemStateMappings:

    def it_returns_work_item_state_mappings_for_each_work_item_source_in_the_project(
            self, project_work_items_source_state_mapping_fixture):
        project = project_work_items_source_state_mapping_fixture

        client = Client(schema)
        query = """
                    query getProjectWorkItemSourceStateMappings($project_key:String!){
                        project(key: $project_key) {
                            workItemsSources (interfaces: [WorkItemStateMappings]){
                                edges {
                                    node {
                                        workItemStateMappings {
                                            state
                                            stateType
                                        }
                                    }
                                }
                            }
                        }
                    }
                  """
        result = client.execute(query, variable_values=dict(project_key=project.key))

        assert result['data']
        work_item_sources = result['data']['project']['workItemsSources']['edges']
        assert len(work_item_sources) == 2
        assert {
                   len(work_item_source['node']['workItemStateMappings'])
                   for work_item_source in work_item_sources
               } == {
                   6, 3
               }
