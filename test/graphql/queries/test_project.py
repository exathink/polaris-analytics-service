# -*- coding: utf-8 -*-

from graphene.test import Client

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
        created_at=get_date("2018-12-02"),
        updated_at=get_date("2018-12-03"),

    )

    yield organization, project, work_items_source, work_items_common

    db.connection().execute("delete  from analytics.work_item_state_transitions")
    db.connection().execute("delete  from analytics.work_item_delivery_cycles")
    db.connection().execute("delete  from analytics.work_items")
    db.connection().execute("delete  from analytics.work_items_source_state_map")
    db.connection().execute("delete  from analytics.work_items_sources")



class TestProjectWorkItemStateTypeCounts:

    def it_returns_cumulative_counts_of_all_state_type_for_work_items_in_the_project(self, api_work_items_import_fixture):
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
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 2',
                    display_id='1001',
                    state='upnext',
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 3',
                    display_id='1002',
                    state='upnext',
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 4',
                    display_id='1004',
                    state='doing',
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 5',
                    display_id='1005',
                    state='doing',
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 6',
                    display_id='1006',
                    state='closed',
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

