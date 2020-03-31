# -*- coding: utf-8 -*-

from graphene.test import Client
from datetime import timedelta
from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *
from polaris.analytics.db.enums import WorkItemsStateType


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
                              tags
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
            assert node['tags']
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
                              tags
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
            assert node['tags'] == work_items_common['tags']
            assert node['url'] == work_items_common['url']
            assert node['earliestCommit'] == get_date("2020-01-29").isoformat()
            assert node['latestCommit'] == get_date("2020-02-05").isoformat()
            assert node['commitCount'] == 2


@pytest.yield_fixture
def api_work_items_import_fixture(org_repo_fixture):
    organization, projects, _ = org_repo_fixture

    project = projects['mercury']
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
            dict(state='backlog', state_type=WorkItemsStateType.backlog.value),
            dict(state='upnext', state_type=WorkItemsStateType.open.value),
            dict(state='doing', state_type=WorkItemsStateType.wip.value),
            dict(state='done', state_type=WorkItemsStateType.complete.value),
            dict(state='closed', state_type=WorkItemsStateType.closed.value),
        ]
    )

    with db.orm_session() as session:
        session.add(organization)
        organization.work_items_sources.append(work_items_source)

    work_items_common = dict(
        is_bug=True,
        work_item_type='issue',
        url='http://foo.com',
        tags=['ares2'],
        description='foo',
        source_id=str(uuid.uuid4()),
    )

    yield organization, project, work_items_source, work_items_common

    db.connection().execute("delete  from analytics.work_item_state_transitions")
    db.connection().execute("delete  from analytics.work_item_delivery_cycle_durations")
    db.connection().execute("delete  from analytics.work_item_delivery_cycles")
    db.connection().execute("delete  from analytics.work_items")
    db.connection().execute("delete  from analytics.work_items_source_state_map")
    db.connection().execute("delete  from analytics.work_items_sources")


class TestProjectWorkItemStateTypeCounts:

    def it_returns_cumulative_counts_of_all_state_type_for_work_items_in_the_project(self,
                                                                                     api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture

        api.import_new_work_items(
            organization_key=organization.key,
            work_item_source_key=work_items_source.key,
            work_item_summaries=[
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 1',
                    display_id='1000',
                    state='backlog',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 2',
                    display_id='1001',
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
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 4',
                    display_id='1004',
                    state='doing',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 5',
                    display_id='1005',
                    state='doing',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 6',
                    display_id='1006',
                    state='closed',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),

            ]
        )

        client = Client(schema)
        query = """
                    query getProjectWorkItemStateTypeCounts($project_key:String!) {
                        project(key: $project_key, interfaces: [WorkItemStateTypeCounts]) {
                            workItemStateTypeCounts {
                                backlog
                                open
                                wip
                                complete
                                closed
                            }
                        }
                    }
                """

        result = client.execute(query, variable_values=dict(project_key=project.key))
        assert 'data' in result
        state_type_counts = result['data']['project']['workItemStateTypeCounts']
        assert state_type_counts['backlog'] == 1
        assert state_type_counts['open'] == 2
        assert state_type_counts['wip'] == 2
        assert state_type_counts['closed'] == 1
        assert state_type_counts['complete'] is None

    def it_returns_a_count_of_unmapped_items(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture


@pytest.yield_fixture
def project_cycle_metrics_test_fixture(api_work_items_import_fixture):
    organization, project, work_items_source, work_items_common = api_work_items_import_fixture
    yield organization, project, work_items_source

    api.import_new_work_items(
        organization_key=organization.key,
        work_item_source_key=work_items_source.key,
        work_item_summaries=[
            dict(
                key=uuid.uuid4().hex,
                name='Issue 1',
                display_id='1000',
                state='backlog',
                created_at=get_date("2018-12-02"),
                updated_at=get_date("2018-12-03"),
                **work_items_common
            ),
            dict(
                key=uuid.uuid4().hex,
                name='Issue 2',
                display_id='1001',
                state='aFunkyState',
                created_at=get_date("2018-12-02"),
                updated_at=get_date("2018-12-03"),
                **work_items_common
            ),
        ]
    )

    client = Client(schema)
    query = """
                        query getProjectWorkItemStateTypeCounts($project_key:String!) {
                            project(key: $project_key, interfaces: [WorkItemStateTypeCounts]) {
                                workItemStateTypeCounts {
                                    backlog
                                    unmapped
                                }
                            }
                        }
                    """

    result = client.execute(query, variable_values=dict(project_key=project.key))
    assert 'data' in result
    state_type_counts = result['data']['project']['workItemStateTypeCounts']
    assert state_type_counts['backlog'] == 1
    assert state_type_counts['unmapped'] == 1


class WorkItemApiHelper:
    def __init__(self, organization, work_items_source):
        self.organization = organization
        self.work_items_source = work_items_source
        self.work_items = None

    def import_work_items(self, work_items):
        self.work_items = work_items
        api.import_new_work_items(
            organization_key=self.organization.key,
            work_item_source_key=self.work_items_source.key,
            work_item_summaries=work_items
        )

    def update_work_items(self, updates):
        for index, state, updated in updates:
            self.work_items[index]['state'] = state
            self.work_items[index]['updated_at'] = updated

        api.update_work_items(self.organization.key, self.work_items_source.key, self.work_items)


class TestProjectCycleMetrics:

    def it_return_correct_results_when_there_are_no_closed_items(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemApiHelper(organization, work_items_source)

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
            for i in range(0, 10)
        ]

        api_helper.import_work_items(work_items)

        api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(0, 'done', start_date + timedelta(days=4))])

        client = Client(schema)
        query = """
                            query getProjectCycleMetrics($project_key:String!, $days: Int, $percentile: Float) {
                                project(key: $project_key, interfaces: [CycleMetrics], 
                                        cycleMetricsDays: $days, cycleMetricsTargetPercentile: $percentile
                                ) {
                                    ... on CycleMetrics {
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
        api_helper = WorkItemApiHelper(organization, work_items_source)

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
            for i in range(0, 10)
        ]

        api_helper.import_work_items(work_items)

        api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(0, 'done', start_date + timedelta(days=4))])
        api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=6))])

        client = Client(schema)
        query = """
                    query getProjectCycleMetrics($project_key:String!, $days: Int, $percentile: Float) {
                        project(key: $project_key, interfaces: [CycleMetrics], 
                                cycleMetricsDays: $days, cycleMetricsTargetPercentile: $percentile
                        ) {
                            ... on CycleMetrics {
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
        api_helper = WorkItemApiHelper(organization, work_items_source)

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
            for i in range(0, 10)
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
                        project(key: $project_key, interfaces: [CycleMetrics], 
                                cycleMetricsDays: $days, cycleMetricsTargetPercentile: $percentile
                        ) {
                            ... on CycleMetrics {
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

    def it_computes_cycle_time_metrics_when_there_are_three_closed_items(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemApiHelper(organization, work_items_source)

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
            for i in range(0, 10)
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
                        project(key: $project_key, interfaces: [CycleMetrics], 
                                cycleMetricsDays: $days, cycleMetricsTargetPercentile: $percentile
                        ) {
                            ... on CycleMetrics {
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
        api_helper = WorkItemApiHelper(organization, work_items_source)

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
            for i in range(0, 10)
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
                        project(key: $project_key, interfaces: [CycleMetrics], 
                                cycleMetricsDays: $days, cycleMetricsTargetPercentile: $percentile
                        ) {
                            ... on CycleMetrics {
                                
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
        api_helper = WorkItemApiHelper(organization, work_items_source)

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
            for i in range(0, 10)
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
                        project(key: $project_key, interfaces: [CycleMetrics], 
                                cycleMetricsDays: $days, cycleMetricsTargetPercentile: $percentile
                        ) {
                            ... on CycleMetrics {

                                workItemsWithNullCycleTime


                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(project_key=project.key, days=30, percentile=0.50))

        assert result['data']
        project = result['data']['project']
        assert project['workItemsWithNullCycleTime'] == 1

    def it_respects_the_days_parameter(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemApiHelper(organization, work_items_source)

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
            for i in range(0, 10)
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
                        project(key: $project_key, interfaces: [CycleMetrics], 
                                cycleMetricsDays: $days, cycleMetricsTargetPercentile: $percentile
                        ) {
                            ... on CycleMetrics {
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
            6,3
        }