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
from polaris.analytics.db.enums import WorkItemsStateType
from test.fixtures.graphql import WorkItemImportApiHelper
from polaris.common.enums import JiraWorkItemType
from polaris.utils.collections import dict_select, dict_to_object


@pytest.yield_fixture
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


class TestProjectContributorCount:

    def it_implements_the_contributor_count_interface(self, projects_import_commits_fixture):
        projects, _ = projects_import_commits_fixture

        client = Client(schema)
        query = """
            query getProjectWorkItems($project_key:String!) {
                project(key: $project_key, interfaces: [ContributorCount]) {
                    contributorCount
                }
            }
        """
        result = client.execute(query, variable_values=dict(project_key=projects['mercury'].key))
        assert 'data' in result
        project = result['data']['project']
        assert project['contributorCount'] == 1

    def it_returns_contributor_counts_for_a_specified_days_interval(self, projects_import_commits_fixture):
        projects, repositories = projects_import_commits_fixture

        api.import_new_commits(
            organization_key=test_organization_key,
            repository_key=repositories['alpha'].key,
            new_commits=[
                dict(
                    source_commit_id='c-XXXX',
                    commit_date=datetime.utcnow(),
                    key=uuid.uuid4().hex,
                    commit_date_tz_offset=0,
                    committer_alias_key=test_contributor_key,
                    author_date=datetime.utcnow(),
                    author_date_tz_offset=0,
                    author_alias_key=test_contributor_key,
                    created_at=datetime.utcnow(),
                    commit_message='a change'
                ),
            ],
            new_contributors=[
                dict(
                    name='Joe Blow',
                    key=test_contributor_key,
                    alias='joe@blow.com'
                )
            ]
        )

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


class TestProjectWorkItems:
    def it_implements_the_work_item_info_interface(self, commit_summary_fixture):
        _, _, _, project = commit_summary_fixture
        client = Client(schema)
        query = """
            query getProjectWorkItems($project_key:String!) {
                project(key: $project_key) {
                    workItems {
                        edges {
                            node {
                              description
                              displayId
                              state
                              workItemType
                              createdAt
                              updatedAt
                              url
                              stateType
                            }
                        }
                    }
                }
            }
        """
        result = client.execute(query, variable_values=dict(project_key=project.key))
        assert 'data' in result
        edges = result['data']['project']['workItems']['edges']
        assert len(edges) == 1
        for node in map(lambda edge: edge['node'], edges):
            assert node['description']
            assert node['displayId']
            assert node['state']
            assert node['workItemType']
            assert node['url']
            assert node['updatedAt']
            assert node['createdAt']
            assert node['stateType']

    def it_implements_the_commit_summary_interface(self, commit_summary_fixture):
        _, _, _, project = commit_summary_fixture
        client = Client(schema)
        query = """
            query getProjectWorkItems($project_key:String!) {
                project(key: $project_key) {
                    workItems(interfaces: [CommitSummary]) {
                        edges {
                            node {
                              description
                              displayId
                              state
                              workItemType
                              url
                              earliestCommit
                              latestCommit
                              commitCount
                            }
                        }
                    }
                }
            }
        """
        result = client.execute(query, variable_values=dict(project_key=project.key))
        assert 'data' in result
        edges = result['data']['project']['workItems']['edges']
        assert len(edges) == 1
        for node in map(lambda edge: edge['node'], edges):
            assert node['description'] == work_items_common['description']
            assert node['displayId'] == "1001"
            assert node['state'] == work_items_common['state']
            assert node['workItemType'] == work_items_common['work_item_type']
            assert node['url'] == work_items_common['url']
            assert node['earliestCommit'] == get_date("2020-01-29").isoformat()
            assert node['latestCommit'] == get_date("2020-02-05").isoformat()
            assert node['commitCount'] == 2

    def it_respects_the_defects_only_parameter(self, api_work_items_import_fixture):
        organization, project, work_items_source, _ = api_work_items_import_fixture

        work_items_common = dict(
            work_item_type='issue',
            url='http://foo.com',
            tags=['ares2'],
            description='foo',
            source_id=str(uuid.uuid4()),
            is_epic=False,
            epic_id=None,
        )

        api.import_new_work_items(
            organization_key=organization.key,
            work_item_source_key=work_items_source.key,
            work_item_summaries=[
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 1',
                    display_id='1000',
                    is_bug=False,
                    state='backlog',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 2',
                    display_id='1001',
                    is_bug=True,
                    state='upnext',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 3',
                    display_id='1002',
                    state='upnext',
                    is_bug=False,
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 4',
                    display_id='1004',
                    state='doing',
                    is_bug=False,
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 5',
                    display_id='1005',
                    state='doing',
                    is_bug=False,
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 6',
                    display_id='1006',
                    state='closed',
                    is_bug=True,
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),

            ]
        )

        client = Client(schema)
        query = """
                    query getProjectDefects($project_key:String!) {
                        project(key: $project_key) {
                            workItems(defectsOnly: true) {
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
        assert 'data' in result
        assert len(result['data']['project']['workItems']['edges']) == 2


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
            epic_id=None,
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

    def it_excludes_jira_subtasks_and_epics_from_cycle_metrics_calculations(self, api_work_items_import_fixture):
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
            epic_id=None,
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


class TestProjectWorkItemDeliveryCycles:

    def it_implements_the_named_node_interface(self, api_work_items_import_fixture):
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

    def it_implements_the_work_item_info_interface(self, api_work_items_import_fixture):
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

    def it_implements_the_delivery_cycle_info_interface(self, api_work_items_import_fixture):
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
            epic_id=None,
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
                **work_items_common
            ),
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue 2',
                display_id='1000',
                state='backlog',
                is_bug=True,
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            ),
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue 3',
                display_id='1000',
                is_bug=True,
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
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

        def it_returns_no_cycle_metrics_when_there_are_no_closed_items(self, api_work_items_import_fixture):
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

    def it_returns_cycle_metrics_for_closed_items(self, api_work_items_import_fixture):
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
            else:
                assert not node['leadTime']
                assert not node['cycleTime']

    def it_returns_cycle_metrics_for_reopened_items(self, api_work_items_import_fixture):
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


@pytest.yield_fixture
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





@pytest.yield_fixture
def api_project_traceability_test_fixture(org_repo_fixture):
    organization, projects, repositories = org_repo_fixture

    with db.orm_session() as session:
        session.add(organization)
        for project in organization.projects:
            work_items_source = WorkItemsSource(
                key=uuid.uuid4(),
                organization_key=organization.key,
                integration_type='jira',
                commit_mapping_scope='repository',
                commit_mapping_scope_key=None,
                organization_id=organization.id,
                **work_items_source_common
            )
            work_items_source.init_state_map(
                [
                    dict(state='backlog', state_type=WorkItemsStateType.backlog.value),
                    dict(state='upnext', state_type=WorkItemsStateType.open.value),
                    dict(state='doing', state_type=WorkItemsStateType.wip.value),
                    dict(state='done', state_type=WorkItemsStateType.complete.value),
                    dict(state='closed', state_type=WorkItemsStateType.closed.value),
                ]
            )
            project.work_items_sources.append(work_items_source)

    contributor_key = uuid.uuid4().hex

    yield dict_to_object(
        dict(
            organization=organization,
            projects=projects,
            repositories=repositories,
            commit_common_fields=dict(
                commit_date_tz_offset=0,
                committer_alias_key=contributor_key,
                author_date=datetime.utcnow(),
                author_date_tz_offset=0,
                author_alias_key=contributor_key,
                created_at=datetime.utcnow(),
                commit_message='a change'

            ),
            work_items_common=dict(
                is_bug=True,
                is_epic=False,
                epic_id=None,
                work_item_type='issue',
                url='http://foo.com',
                tags=['ares2'],
                description='foo',
                source_id=str(uuid.uuid4()),
            ),
            contributors=[
                dict(
                    name='Joe Blow',
                    key=contributor_key,
                    alias='joe@blow.com'
                )
            ]
        )
    )

    db.connection().execute("delete  from analytics.work_items_commits")
    db.connection().execute("delete  from analytics.work_item_state_transitions")
    db.connection().execute("delete  from analytics.work_item_delivery_cycle_durations")
    db.connection().execute("delete  from analytics.work_item_delivery_cycles")
    db.connection().execute("delete  from analytics.work_items")
    db.connection().execute("delete  from analytics.work_items_source_state_map")
    db.connection().execute("delete  from analytics.work_items_sources")
    db.connection().execute("delete  from analytics.commits")
    db.connection().execute("delete  from analytics.contributor_aliases")
    db.connection().execute("delete  from analytics.contributors")



def add_work_item_commits(work_items_commits):
    with db.orm_session() as session:
        for work_item_key, commit_key in work_items_commits:
            work_item = WorkItem.find_by_work_item_key(session, work_item_key)
            commit = Commit.find_by_commit_key(session, commit_key)
            work_item.commits.append(commit)


class TestProjectTraceabilityTrends:
    project_traceability_query = """
        query getProjectTraceabilityTrends(
            $project_key:String!, 
            $days: Int!, 
            $window: Int!,
            $sample: Int
        ) {
            project(
                key: $project_key, 
                interfaces: [TraceabilityTrends], 
                traceabilityTrendsArgs: {
                    days: $days,
                    measurementWindow: $window,
                    samplingFrequency: $sample,
                }

            ) {
                traceabilityTrends {
                    measurementDate
                    traceability
                    specCount
                    nospecCount
                    totalCommits
                }
            }
        }
    """

    # Base cases: Test that nothing is dropped when there are no work items or commits. We should
    # see a series with one point for each date in the interval with zero values for all the metrics.
    def it_reports_all_zeros_when_there_are_no_work_items_or_commits_in_the_project_and_a_single_sample_in_window(self,
                                                                                                                  api_project_traceability_test_fixture):
        fixture = api_project_traceability_test_fixture

        client = Client(schema)

        result = client.execute(self.project_traceability_query, variable_values=dict(
            project_key=fixture.projects['mercury'].key,
            days=30,
            window=30,
            sample=30
        ))
        assert result['data']
        traceability_trends = result['data']['project']['traceabilityTrends']
        # there should one measurement per sample in the measurement window, including each end point.
        assert len(traceability_trends) == 2
        assert [
                   dict_select(measurement, ['traceability', 'specCount', 'nospecCount', 'totalCommits'])
                   for measurement in traceability_trends
               ] == [
                   dict(
                       # beginning of window
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       # end of window
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   )
               ]

    def it_reports_all_zeros_when_there_are_no_work_items_or_commits_in_the_project_and_multiple_samples_in_window(self,
                                                                                                                   api_project_traceability_test_fixture):
        fixture = api_project_traceability_test_fixture

        client = Client(schema)

        result = client.execute(self.project_traceability_query, variable_values=dict(
            project_key=fixture.projects['mercury'].key,
            days=30,
            window=30,
            sample=7
        ))
        assert result['data']
        traceability_trends = result['data']['project']['traceabilityTrends']
        # there should one measurement per sample in the measurement window, including each end point.
        assert len(traceability_trends) == 5
        assert [
                   dict_select(measurement, ['traceability', 'specCount', 'nospecCount', 'totalCommits'])
                   for measurement in traceability_trends
               ] == [
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   )
                   for _ in range(0, 5)
               ]

    def it_reports_commit_and_nospec_counts_when_there_are_commits_in_the_project_and_zeros_for_the_rest(self,
                                                                                                         api_project_traceability_test_fixture
                                                                                                         ):
        fixture = api_project_traceability_test_fixture

        start_date = datetime.utcnow() - timedelta(days=10)
        api.import_new_commits(
            organization_key=fixture.organization.key,
            repository_key=fixture.projects['mercury'].repositories[0].key,
            new_commits=[
                dict(
                    source_commit_id='a-XXXX',
                    # one commit 10 days from the end of the window
                    commit_date=start_date,
                    key=uuid.uuid4().hex,
                    **fixture.commit_common_fields
                ),
                dict(
                    source_commit_id='a-YYYY',
                    # next commit 20 days from end of the window
                    commit_date=start_date - timedelta(days=10),
                    key=uuid.uuid4().hex,
                    **fixture.commit_common_fields
                )
            ],
            new_contributors=fixture.contributors
        )

        client = Client(schema)

        result = client.execute(self.project_traceability_query, variable_values=dict(
            project_key=fixture.projects['mercury'].key,
            days=30,
            window=15,
            sample=7
        ))
        assert result['data']
        traceability_trends = result['data']['project']['traceabilityTrends']
        # there should one measurement per sample in the measurement window, including each end point.
        assert len(traceability_trends) == 5

        assert [
                   dict_select(measurement, ['traceability', 'specCount', 'nospecCount', 'totalCommits'])
                   for measurement in traceability_trends
               ] == [
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=1,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=2,
                       totalCommits=2
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=1,
                       totalCommits=1
                   )
               ]

    def it_reports_commit_and_nospec_counts_when_there_are_commits_that_are_not_associated_with_any_work_items(self, api_project_traceability_test_fixture):
        fixture = api_project_traceability_test_fixture

        # same setup as last one, but we are going to add some work items but not associate them to any commits
        start_date = datetime.utcnow() - timedelta(days=10)
        api.import_new_commits(
            organization_key=fixture.organization.key,
            repository_key=fixture.projects['mercury'].repositories[0].key,
            new_commits=[
                dict(
                    source_commit_id='a-XXXX',
                    # one commit 10 days from the end of the window
                    commit_date=start_date,
                    key=uuid.uuid4().hex,
                    **fixture.commit_common_fields
                ),
                dict(
                    source_commit_id='a-YYYY',
                    # next commit 20 days from end of the window
                    commit_date=start_date - timedelta(days=10),
                    key=uuid.uuid4().hex,
                    **fixture.commit_common_fields
                )
            ],
            new_contributors=fixture.contributors
        )

        api_helper = WorkItemImportApiHelper(fixture.organization, fixture.projects['mercury'].work_items_sources[0])

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **fixture.work_items_common
            )
            for i in range(0, 3)
        ]

        # the default fixture sets everything to is_bug=True so we flip to set up this test.
        work_items[0]['is_bug'] = False
        work_items[1]['is_bug'] = False

        api_helper.import_work_items(work_items)

        client = Client(schema)

        result = client.execute(self.project_traceability_query, variable_values=dict(
            project_key=fixture.projects['mercury'].key,
            days=30,
            window=15,
            sample=7
        ))
        assert result['data']
        traceability_trends = result['data']['project']['traceabilityTrends']
        # assertions here are the same as the last test, as adding work items should have no impact on the metrics

        # there should one measurement per sample in the measurement window, including each end point.
        assert len(traceability_trends) == 5

        assert [
                   dict_select(measurement, ['traceability', 'specCount', 'nospecCount', 'totalCommits'])
                   for measurement in traceability_trends
               ] == [
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=1,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=2,
                       totalCommits=2
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=1,
                       totalCommits=1
                   )
               ]

    def it_reports_traceability_and_spec_count_when_there_are_commits_associated_with_work_items(self,
                                                                                                 api_project_traceability_test_fixture):
        fixture = api_project_traceability_test_fixture

        # same setup as last one, but we are going to add some work items but not associate them to any commits
        start_date = datetime.utcnow() - timedelta(days=10)
        new_commits = [
            dict(
                source_commit_id='a-XXXX',
                # one commit 10 days from the end of the window
                commit_date=start_date,
                key=uuid.uuid4().hex,
                **fixture.commit_common_fields
            ),
            dict(
                source_commit_id='a-YYYY',
                # next commit 20 days from end of the window
                commit_date=start_date - timedelta(days=10),
                key=uuid.uuid4().hex,
                **fixture.commit_common_fields
            )
        ]
        api.import_new_commits(
            organization_key=fixture.organization.key,
            repository_key=fixture.projects['mercury'].repositories[0].key,
            new_commits=new_commits,
            new_contributors=fixture.contributors
        )

        api_helper = WorkItemImportApiHelper(fixture.organization, fixture.projects['mercury'].work_items_sources[0])

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **fixture.work_items_common
            )
            for i in range(0, 3)
        ]

        # the default fixture sets everything to is_bug=True so we flip to set up this test.
        work_items[0]['is_bug'] = False
        work_items[1]['is_bug'] = False

        api_helper.import_work_items(work_items)

        add_work_item_commits([(work_items[0]['key'], new_commits[0]['key'])])

        client = Client(schema)

        result = client.execute(self.project_traceability_query, variable_values=dict(
            project_key=fixture.projects['mercury'].key,
            days=30,
            window=15,
            sample=7
        ))
        assert result['data']
        traceability_trends = result['data']['project']['traceabilityTrends']
        # assertions here are the same as the last test, as adding work items should have no impact on the metrics

        # there should one measurement per sample in the measurement window, including each end point.
        assert len(traceability_trends) == 5

        assert [
                   dict_select(measurement, ['traceability', 'specCount', 'nospecCount', 'totalCommits'])
                   for measurement in traceability_trends
               ] == [
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=1,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0.5,
                       specCount=1,
                       nospecCount=1,
                       totalCommits=2
                   ),
                   dict(
                       traceability=1.0,
                       specCount=1,
                       nospecCount=0,
                       totalCommits=1
                   )
               ]

    def it_reports_traceability_and_spec_counts_correctly_when_repositories_are_shared_across_projects(self,
                                                                                                 api_project_traceability_test_fixture):
        fixture = api_project_traceability_test_fixture

        # same setup as last one, but we are going to add some work items but not associate them to any commits
        start_date = datetime.utcnow() - timedelta(days=10)
        new_commits = [
            dict(
                source_commit_id='a-XXXX',
                # one commit 10 days from the end of the window
                commit_date=start_date,
                key=uuid.uuid4().hex,
                **fixture.commit_common_fields
            ),
            dict(
                source_commit_id='a-YYYY',
                # next commit 20 days from end of the window
                commit_date=start_date - timedelta(days=10),
                key=uuid.uuid4().hex,
                **fixture.commit_common_fields
            )
        ]
        # alpha is a repo shared between projects mercury and venus
        alpha = fixture.repositories['alpha']
        api.import_new_commits(
            organization_key=fixture.organization.key,
            repository_key=alpha.key,
            new_commits=new_commits,
            new_contributors=fixture.contributors
        )

        api_helper = WorkItemImportApiHelper(fixture.organization, fixture.projects['mercury'].work_items_sources[0])

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id='1000',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **fixture.work_items_common
            )
            for i in range(0, 3)
        ]

        # the default fixture sets everything to is_bug=True so we flip to set up this test.
        work_items[0]['is_bug'] = False
        work_items[1]['is_bug'] = False

        api_helper.import_work_items(work_items)

        add_work_item_commits([(work_items[0]['key'], new_commits[0]['key'])])

        client = Client(schema)

        result = client.execute(self.project_traceability_query, variable_values=dict(
            project_key=fixture.projects['mercury'].key,
            days=30,
            window=15,
            sample=7
        ))
        assert result['data']
        traceability_trends = result['data']['project']['traceabilityTrends']

        # the first set of assertions tests the metrics for mercury, which should not have changed in
        # from the last test.

        # there should one measurement per sample in the measurement window, including each end point.
        assert len(traceability_trends) == 5

        assert [
                   dict_select(measurement, ['traceability', 'specCount', 'nospecCount', 'totalCommits'])
                   for measurement in traceability_trends
               ] == [
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=1,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0.5,
                       specCount=1,
                       nospecCount=1,
                       totalCommits=2
                   ),
                   dict(
                       traceability=1.0,
                       specCount=1,
                       nospecCount=0,
                       totalCommits=1
                   )
               ]

        # now we test the metrics for venus. which shares commits with mercury, but does not have  work items, so its
        # spec count should be zero and spec_count + non_spec_count should be < total commit count

        result = client.execute(self.project_traceability_query, variable_values=dict(
            project_key=fixture.projects['venus'].key,
            days=30,
            window=15,
            sample=7
        ))
        assert result['data']
        traceability_trends = result['data']['project']['traceabilityTrends']

        # the first set of assertions tests the metrics for mercury, which should not have changed in
        # from the last test.

        # there should one measurement per sample in the measurement window, including each end point.
        assert len(traceability_trends) == 5

        assert [
                   dict_select(measurement, ['traceability', 'specCount', 'nospecCount', 'totalCommits'])
                   for measurement in traceability_trends
               ] == [
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=0
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=1,
                       totalCommits=1
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=1,
                       totalCommits=2
                   ),
                   dict(
                       traceability=0.0,
                       specCount=0,
                       nospecCount=0,
                       totalCommits=1
                   )
               ]
