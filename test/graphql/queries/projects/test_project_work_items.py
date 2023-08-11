# -*- coding: utf-8 -*-
# Copyright: © Exathink, LLC (2011-2020) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

import uuid
import pytest
from datetime import datetime, timedelta
from graphene.test import Client
from polaris.analytics.service.graphql import schema
from polaris.analytics.db import api
from test.fixtures.graphql import *
from test.fixtures.graphql import WorkItemImportApiHelper


class TestProjectWorkItemsInterfaces(CommitSummaryFixtureTest):
    class TestInterfaces:
        def it_implements_the_work_item_info_interface(self, setup):
            fixture = setup
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
            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
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

        def it_implements_the_commit_summary_interface(self, setup):
            fixture = setup
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
            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
            assert 'data' in result
            edges = result['data']['project']['workItems']['edges']
            assert len(edges) == 1
            work_items_common = fixture.work_items_common
            for node in map(lambda edge: edge['node'], edges):
                assert node['description'] == work_items_common['description']
                assert node['displayId'] == "1001"
                assert node['state'] == work_items_common['state']
                assert node['workItemType'] == work_items_common['work_item_type']
                assert node['url'] == work_items_common['url']
                assert node['earliestCommit'] == get_date("2020-01-29").isoformat()
                assert node['latestCommit'] == get_date("2020-02-05").isoformat()
                assert node['commitCount'] == 2


class TestProjectWorkItemsParameters(WorkItemApiImportTest):
    class TestParameters:

        def it_respects_the_defects_only_parameter(self, setup):
            fixture = setup

            work_items_common = dict(
                work_item_type='issue',
                url='http://foo.com',
                tags=['ares2'],
                description='foo',
                source_id=str(uuid.uuid4()),
                is_epic=False,
                parent_id=None,
            )

            api.import_new_work_items(
                organization_key=fixture.organization.key,
                work_item_source_key=fixture.work_items_source.key,
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
            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
            assert 'data' in result
            assert len(result['data']['project']['workItems']['edges']) == 2

        def it_respects_the_specs_only_flag(self, setup):
            fixture = setup
            api_helper = WorkItemImportApiHelper(fixture.organization, fixture.work_items_source)

            start_date = datetime.utcnow() - timedelta(days=10)
            work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name=f'Issue {i}',
                    state='backlog',
                    display_id='1000',
                    created_at=start_date,
                    updated_at=start_date,
                    **fixture.work_items_common
                )
                for i in range(0, 3)
            ]
            api_helper.import_work_items(work_items)
            api_helper.update_delivery_cycles(([(0, dict(property='commit_count', value=3))]))

            client = Client(schema)
            query = """
                    query getProjectSpecs($project_key:String!) {
                        project(key: $project_key) {
                            workItems(specsOnly: true) {
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
            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
            assert 'data' in result
            assert len(result['data']['project']['workItems']['edges']) == 1


class TestProjectWorkItemFilteringByTags(WorkItemApiImportTest):
    class TestWorkItemTagFiltering:
        @pytest.fixture()
        def setup(self, setup):
            fixture = setup

            common_fields = dict(
                work_item_type='issue',
                url='http://foo.com',
                description='foo',
                source_id=str(uuid.uuid4()),
                is_epic=False,
                parent_id=None,
                is_bug=True,
                state='upnext',
                created_at=get_date("2018-12-02"),
                updated_at=get_date("2018-12-03"),
            )

            api.import_new_work_items(
                organization_key=fixture.organization.key,
                work_item_source_key=fixture.work_items_source.key,
                work_item_summaries=[
                    dict(
                        key=uuid.uuid4().hex,
                        name='Issue 1',
                        display_id='1000',
                        tags=['enhancement', 'feature1'],
                        **common_fields
                    ),
                    dict(
                        key=uuid.uuid4().hex,
                        name='Issue 2',
                        display_id='1001',
                        tags=[],
                        **common_fields
                    ),
                    dict(
                        key=uuid.uuid4().hex,
                        name='Issue 3',
                        display_id='1002',
                        tags=['escaped', 'feature2'],
                        **common_fields
                    ),
                    dict(
                        key=uuid.uuid4().hex,
                        name='Issue 4',
                        display_id='1004',
                        tags=['new_feature'],
                        **common_fields
                    ),
                    dict(
                        key=uuid.uuid4().hex,
                        name='Issue 5',
                        display_id='1005',
                        tags=['enhancement', 'feature2'],
                        **common_fields
                    ),
                    dict(
                        key=uuid.uuid4().hex,
                        name='Issue 6',
                        display_id='1006',
                        tags=['escaped', 'feature1'],
                        **common_fields
                    ),

                ]
            )

            query = """
                    query getProjectWorkItemsByTag($project_key:String!, $tags:[String]!) {
                        project(key: $project_key) {
                            workItems(tags: $tags){
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
            yield Fixture(
                parent=fixture,
                query=query
            )

        def it_filters_work_items_by_a_single_tag(self, setup):
            fixture = setup

            client = Client(schema)
            result = client.execute(fixture.query,
                                    variable_values=dict(project_key=fixture.project.key, tags=['enhancement']))
            assert not result.get('errors')
            assert result['data']
            edges = result['data']['project']['workItems']['edges']
            assert len(edges) == 2

            assert {
                       edge['node']['name']
                       for edge in edges
                   } == {
                       'Issue 1',
                       'Issue 5'
                   }

        def it_filters_work_items_by_a_multiple_tags(self, setup):
            fixture = setup

            client = Client(schema)
            result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project.key,
                                                                        tags=['enhancement', 'feature1']))
            assert not result.get('errors')
            assert result['data']
            edges = result['data']['project']['workItems']['edges']
            assert len(edges) == 3
            assert {
                       edge['node']['name']
                       for edge in edges
                   } == {
                       'Issue 1',
                       'Issue 5',
                       'Issue 6'
                   }

        def it_returns_multiple_matches_for_multiple_tags(self, setup):
            fixture = setup

            client = Client(schema)
            result = client.execute(fixture.query,
                                    variable_values=dict(project_key=fixture.project.key, tags=['escaped', 'feature2']))
            assert not result.get('errors')
            assert result['data']
            edges = result['data']['project']['workItems']['edges']
            assert len(edges) == 3
            assert {
                       edge['node']['name']
                       for edge in edges
                   } == {
                       'Issue 3',
                       'Issue 5',
                       'Issue 6'
                   }

        def it_returns_all_items_if_the_tag_list_is_empty(self, setup):
            fixture = setup

            client = Client(schema)
            result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project.key, tags=[]))
            assert not result.get('errors')
            assert result['data']
            edges = result['data']['project']['workItems']['edges']
            assert len(edges) == 6

        def it_returns_no_items_if_there_are_no_matches(self, setup):
            fixture = setup

            client = Client(schema)
            result = client.execute(fixture.query,
                                    variable_values=dict(project_key=fixture.project.key, tags=['random string']))
            assert not result.get('errors')
            assert result['data']
            edges = result['data']['project']['workItems']['edges']
            assert len(edges) == 0

class TestProjectWorkItemFilteringByReleases(WorkItemApiImportTest):
    class TestWorkItemReleaseFiltering:
        @pytest.fixture()
        def setup(self, setup):
            fixture = setup

            common_fields = dict(
                work_item_type='issue',
                url='http://foo.com',
                description='foo',
                source_id=str(uuid.uuid4()),
                is_epic=False,
                parent_id=None,
                is_bug=True,
                state='upnext',
                created_at=get_date("2018-12-02"),
                updated_at=get_date("2018-12-03"),
            )

            result = api.import_new_work_items(
                organization_key=fixture.organization.key,
                work_item_source_key=fixture.work_items_source.key,
                work_item_summaries=[
                    dict(
                        key=uuid.uuid4().hex,
                        name='Issue 1',
                        display_id='1000',
                        tags=[],
                        releases=['1.0.1', '1.0.2'],
                        **common_fields
                    ),
                    dict(
                        key=uuid.uuid4().hex,
                        name='Issue 2',
                        display_id='1001',
                        releases=[],
                        tags=[],
                        **common_fields
                    ),
                    dict(
                        key=uuid.uuid4().hex,
                        name='Issue 3',
                        display_id='1002',
                        releases=['1.0.1'],
                        tags=[],
                        **common_fields
                    ),
                    dict(
                        key=uuid.uuid4().hex,
                        name='Issue 4',
                        display_id='1004',
                        releases=['1.0.2'],
                        tags=[],
                        **common_fields
                    ),
                    dict(
                        key=uuid.uuid4().hex,
                        name='Issue 5',
                        display_id='1005',
                        releases=['1.0.3'],
                        tags=[],
                        **common_fields
                    ),
                    dict(
                        key=uuid.uuid4().hex,
                        name='Issue 6',
                        display_id='1006',
                        releases=['1.0.3'],
                        tags=[],
                        **common_fields
                    ),

                ]
            )
            assert result.get('success')

            query = """
                    query getProjectWorkItemsByRelease($project_key:String!, $release:String) {
                        project(key: $project_key) {
                            workItems(release: $release){
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
            yield Fixture(
                parent=fixture,
                query=query
            )

        def it_filters_work_items_by_a_single_release(self, setup):
            fixture = setup

            client = Client(schema)
            result = client.execute(fixture.query,
                                    variable_values=dict(project_key=fixture.project.key, release='1.0.1'))
            assert not result.get('errors')
            assert result['data']
            edges = result['data']['project']['workItems']['edges']
            assert len(edges) == 2

            assert {
                       edge['node']['name']
                       for edge in edges
                   } == {
                       'Issue 1',
                       'Issue 3'
                   }


        def it_returns_all_items_if_releases_is_empty(self, setup):
            fixture = setup

            client = Client(schema)
            result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project.key, release=None))
            assert not result.get('errors')
            assert result['data']
            edges = result['data']['project']['workItems']['edges']
            assert len(edges) == 6


        def it_returns_no_items_if_there_are_no_matches(self, setup):
            fixture = setup

            client = Client(schema)
            result = client.execute(fixture.query,
                                    variable_values=dict(project_key=fixture.project.key, release='random string'))
            assert not result.get('errors')
            assert result['data']
            edges = result['data']['project']['workItems']['edges']
            assert len(edges) == 0

class TestProjectMovedWorkItems(WorkItemApiImportTest):
    class TestMovedWorkItems:
        def it_does_not_return_the_work_items_moved_from_source(self, setup):
            fixture = setup
            api_helper = WorkItemImportApiHelper(fixture.organization, fixture.work_items_source)

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
            api_helper.import_work_items(work_items)
            api_helper.update_work_item_attributes(0, dict(is_moved_from_current_source=True))

            client = Client(schema)
            query = """
                    query getProjectDefects($project_key:String!) {
                        project(key: $project_key) {
                            workItems {
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
            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
            assert 'data' in result
            assert len(result['data']['project']['workItems']['edges']) == 2

        def it_returns_the_work_items_moved_from_source_when_filter_is_false(self, setup):
            fixture = setup
            api_helper = WorkItemImportApiHelper(fixture.organization, fixture.work_items_source)

            work_items_common = dict(
                work_item_type='issue',
                url='http://foo.com',
                tags=['ares2'],
                description='foo',
                source_id=str(uuid.uuid4()),
                is_epic=False,
                parent_id=None,
            )

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
            api_helper.import_work_items(work_items)
            api_helper.update_work_item_attributes(0, dict(is_moved_from_current_source=True))

            client = Client(schema)
            query = """
                    query getProjectDefects($project_key:String!) {
                        project(key: $project_key) {
                            workItems(suppressMovedItems: false) {
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
            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
            assert 'data' in result
            assert len(result['data']['project']['workItems']['edges']) == 3


class TestProjectEpicWorkItems(WorkItemApiImportTest):
    class TestEpicWorkItems:
        @pytest.fixture()
        def setup(self, setup):
            fixture = setup
            api_helper = fixture.api_helper
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
                for i in range(0, 4)
            ]

            api_helper.import_work_items(work_items)
            # Convert 1 work item to epic and 2 others as its children
            api_helper.update_work_item_attributes(0, dict(is_epic=True, budget=3.0, name='Only Epic'))
            with db.orm_session() as session:
                epic = WorkItem.find_by_work_item_key(session, work_items[0]['key'])
            api_helper.update_work_item_attributes(1, dict(parent_id=epic.id, budget=1.0))
            api_helper.update_work_item_attributes(2, dict(parent_id=epic.id, budget=1.0))
            api_helper.update_work_item_attributes(3, dict(budget=3.0))

            # Add contributors
            contributor_info = [
                dict(key=uuid.uuid4(), name='joe@blow.com'),
                dict(key=uuid.uuid4(), name='ida@jay.com')
            ]
            with db.create_session() as session:
                for c_info in contributor_info:
                    contributor_id = session.connection.execute(
                        contributors.insert(
                            dict(
                                key=c_info['key'],
                                name=c_info['name']
                            )
                        )
                    ).inserted_primary_key[0]

                    contributor_alias_id = session.connection.execute(
                        contributor_aliases.insert(
                            dict(
                                source_alias='joe@blow.com',
                                key=c_info['key'],
                                name=c_info['name'],
                                contributor_id=contributor_id,
                                source='vcs'
                            )
                        )
                    ).inserted_primary_key[0]
                    c_info['id'] = contributor_id
                    c_info['alias_id'] = contributor_alias_id

            yield Fixture(
                parent=fixture,
                work_items=work_items,
                contributors=contributor_info,
                start_date=start_date,
                epic=epic
            )

        class TestWithoutWorkItemCommits:

            @pytest.fixture()
            def setup(self, setup):
                fixture = setup
                query = """
                    query getProjectEpicWorkItems($project_key:String!) {
                        project(key: $project_key) {
                            workItems(
                                interfaces: [EpicNodeRef, ImplementationCost, DevelopmentProgress],
                                includeEpics: true,
                                activeWithinDays: 90, 
                                includeSubTasks: false
                                ) {
                                edges {
                                    node {
                                      id
                                      name
                                      key
                                      displayId
                                      epicName
                                      epicKey
                                      budget
                                      effort
                                      authorCount
                                      duration
                                      closed
                                      startDate
                                      endDate
                                      lastUpdate
                                      elapsed
                                    }
                                }
                            }
                        }
                    }
                """

                yield Fixture(
                    parent=fixture,
                    query=query
                )

            def it_returns_correct_epic_node_refs_budget_and_other_default_values(self, setup):
                fixture = setup
                client = Client(schema)
                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project.key))

                assert result['data']
                all_work_items = result['data']['project']['workItems']['edges']
                assert len(all_work_items) == 4
                for wi in all_work_items:
                    if wi['node']['key'] == str(uuid.UUID(fixture.work_items[1]['key'])) or wi['node']['key'] == str(
                            uuid.UUID(fixture.work_items[2]['key'])):
                        assert wi['node']['epicKey'] == str(fixture.epic.key)
                        assert wi['node']['epicName'] == str(fixture.epic.name)
                        assert wi['node']['budget'] == 1.0
                    else:
                        assert not wi['node']['epicKey']
                        assert not wi['node']['epicName']
                        assert wi['node']['budget'] == 3.0
                    assert wi['node']['effort'] is None
                    assert wi['node']['duration'] is None
                    assert wi['node']['authorCount'] == 0
                    assert wi['node']['closed'] == False
                    assert wi['node']['startDate'] == fixture.start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
                    assert wi['node']['endDate'] is None
                    assert int(wi['node']['elapsed']) == 10

        class TestWithWorkItemCommits:

            @pytest.fixture()
            def setup(self, setup):
                # Add only work item delivery cycle contributors and update work item delivery cycles
                fixture = setup

                query = """
                    query getProjectEpicWorkItems($project_key:String!) {
                        project(key: $project_key) {
                            workItems(
                                interfaces: [EpicNodeRef, ImplementationCost, DevelopmentProgress],
                                includeEpics: true,
                                activeWithinDays: 90, 
                                includeSubTasks: false
                                ) {
                                edges {
                                    node {
                                      id
                                      name
                                      key
                                      displayId
                                      epicName
                                      epicKey
                                      budget
                                      effort
                                      authorCount
                                      duration
                                      closed
                                      startDate
                                      endDate
                                      lastUpdate
                                      elapsed
                                    }
                                }
                            }
                        }
                    }
                """

                api_helper = fixture.api_helper
                # Update work items for effort
                api_helper.update_work_item(1, dict(effort=3.5))
                api_helper.update_work_item(2, dict(effort=3.5))
                api_helper.update_work_item(3, dict(effort=3.5))
                # Update delivery cycle for latest_commit and earliest_commit
                work_items = fixture.work_items
                commit_1_date = datetime.utcnow() - timedelta(days=5)
                commit_2_date = datetime.utcnow() - timedelta(days=6)
                commit_3_date = datetime.utcnow() - timedelta(days=7)
                commit_4_date = datetime.utcnow() - timedelta(days=8)
                commit_5_date = datetime.utcnow() - timedelta(days=9)
                api_helper.update_delivery_cycle(1, dict(latest_commit=commit_1_date, earliest_commit=commit_3_date,
                                                         commit_count=2))
                api_helper.update_delivery_cycle(2, dict(latest_commit=commit_2_date, earliest_commit=commit_4_date,
                                                         commit_count=2))
                api_helper.update_delivery_cycle(3, dict(latest_commit=commit_5_date, earliest_commit=commit_5_date,
                                                         commit_count=1))

                # Contributor 1 to work item 1
                api_helper.update_delivery_cycle_contributors(
                    1,
                    dict(
                        contributor_alias_id=fixture.contributors[0]['alias_id'],
                        total_lines_as_author=20,
                        total_lines_as_reviewer=20
                    )
                )
                # Contributor 2 to work item 2
                api_helper.update_delivery_cycle_contributors(
                    2,
                    dict(
                        contributor_alias_id=fixture.contributors[1]['alias_id'],
                        total_lines_as_author=10,
                        total_lines_as_reviewer=10
                    )
                )
                # Contributor 1 to work item 3
                api_helper.update_delivery_cycle_contributors(
                    3,
                    dict(
                        contributor_alias_id=fixture.contributors[0]['alias_id'],
                        total_lines_as_author=10,
                        total_lines_as_reviewer=10
                    )
                )

                yield Fixture(
                    parent=fixture,
                    query=query
                )

            class TestWithAllOpenDeliveryCycles:
                def it_returns_all_correct_data_points_for_all_interfaces(self, setup):
                    fixture = setup

                    client = Client(schema)
                    result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project.key))

                    assert result['data']
                    all_work_items = result['data']['project']['workItems']['edges']
                    assert len(all_work_items) == 4
                    for wi in all_work_items:
                        # For epic
                        if wi['node']['key'] == str(fixture.epic.key):
                            assert not wi['node']['epicKey']
                            assert not wi['node']['epicName']
                            assert wi['node']['budget'] == 3.0
                            assert wi['node']['effort'] == 7
                            assert wi['node']['authorCount'] == 2
                            assert wi['node']['duration'] > 0
                            assert wi['node']['closed'] == False
                            assert wi['node']['startDate'] == fixture.start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
                            assert wi['node']['endDate'] is None
                            assert int(wi['node']['elapsed']) == 10
                        # for epic children
                        if wi['node']['key'] == str(uuid.UUID(fixture.work_items[1]['key'])) \
                                or wi['node']['key'] == str(
                            uuid.UUID(fixture.work_items[2]['key'])):
                            assert wi['node']['epicKey'] == str(fixture.epic.key)
                            assert wi['node']['epicName'] == str(fixture.epic.name)
                            assert wi['node']['budget'] == 1.0
                            assert wi['node']['effort'] == 3.5
                            assert wi['node']['authorCount'] == 1
                            assert wi['node']['duration'] > 0
                            assert wi['node']['closed'] == False
                            assert wi['node']['startDate'] == fixture.start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
                            assert wi['node']['endDate'] is None
                            assert int(wi['node']['elapsed']) == 10
                        # the uncategorized
                        if wi['node']['key'] == str(uuid.UUID(fixture.work_items[3]['key'])):
                            assert not wi['node']['epicKey']
                            assert not wi['node']['epicName']
                            assert wi['node']['budget'] == 3.0
                            assert wi['node']['effort'] == 3.5
                            assert wi['node']['authorCount'] == 1
                            assert wi['node']['duration'] == 0.0
                            assert wi['node']['closed'] == False
                            assert wi['node']['startDate'] == fixture.start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
                            assert wi['node']['endDate'] is None
                            assert int(wi['node']['elapsed']) == 10

            class TestWithOneClosedDeliveryCycle:

                @pytest.fixture()
                def setup(self, setup):
                    fixture = setup

                    api_helper = fixture.api_helper
                    api_helper.update_delivery_cycle(1, dict(end_date=datetime.utcnow()))

                    yield Fixture(
                        parent=fixture
                    )

                def it_returns_all_correct_data_points_for_implementation_cost(self, setup):
                    fixture = setup

                    client = Client(schema)
                    result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project.key))

                    assert result['data']
                    all_work_items = result['data']['project']['workItems']['edges']
                    assert len(all_work_items) == 4
                    for wi in all_work_items:
                        if wi['node']['key'] == str(fixture.epic.key):
                            assert wi['node']['effort'] == 7
                            assert wi['node']['authorCount'] == 2
                            assert wi['node']['closed'] == False
                        else:
                            assert wi['node']['effort'] == 3.5
                            assert wi['node']['authorCount'] == 1
                        assert wi['node']['startDate'] == fixture.start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
                        assert int(wi['node']['elapsed']) == 10
                        if wi['node']['key'] == str(uuid.UUID(fixture.work_items[1]['key'])):
                            assert wi['node']['closed'] == True
                            assert wi['node']['endDate'] is not None

            class TestWithIncludeEpicsFalse:

                @pytest.fixture()
                def setup(self, setup):
                    fixture = setup
                    query = """
                        query getProjectEpicWorkItems($project_key:String!) {
                            project(key: $project_key) {
                                workItems(
                                    interfaces: [EpicNodeRef, ImplementationCost, DevelopmentProgress],
                                    includeEpics: false,
                                    activeWithinDays: 90, 
                                    includeSubTasks: false
                                    ) {
                                    edges {
                                        node {
                                          id
                                          name
                                          key
                                          displayId
                                          epicName
                                          epicKey
                                          budget
                                          effort
                                          authorCount
                                          duration
                                          closed
                                          startDate
                                          endDate
                                          lastUpdate
                                          elapsed
                                        }
                                    }
                                }
                            }
                        }
                    """

                    yield Fixture(
                        parent=fixture,
                        noepics_query=query
                    )

                def it_does_not_return_epic_but_others(self, setup):
                    fixture = setup

                    client = Client(schema)
                    result = client.execute(fixture.noepics_query,
                                            variable_values=dict(project_key=fixture.project.key))

                    assert result['data']
                    all_work_items = result['data']['project']['workItems']['edges']
                    assert len(all_work_items) == 3
                    for wi in all_work_items:
                        # for epic children
                        assert not wi['node']['key'] == str(fixture.epic.key)
                        if wi['node']['key'] == str(uuid.UUID(fixture.work_items[1]['key'])) \
                                or wi['node']['key'] == str(
                            uuid.UUID(fixture.work_items[2]['key'])):
                            assert wi['node']['epicKey'] == str(fixture.epic.key)
                            assert wi['node']['epicName'] == str(fixture.epic.name)
                            assert wi['node']['budget'] == 1.0
                            assert wi['node']['effort'] == 3.5
                            assert wi['node']['authorCount'] == 1
                            assert wi['node']['duration'] > 0
                            assert wi['node']['closed'] == False
                            assert wi['node']['startDate'] == fixture.start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
                            assert wi['node']['endDate'] is None
                            assert int(wi['node']['elapsed']) == 10
                        # the uncategorized
                        if wi['node']['key'] == str(uuid.UUID(fixture.work_items[3]['key'])):
                            assert not wi['node']['epicKey']
                            assert not wi['node']['epicName']
                            assert wi['node']['budget'] == 3.0
                            assert wi['node']['effort'] == 3.5
                            assert wi['node']['authorCount'] == 1
                            assert wi['node']['duration'] == 0.0
                            assert wi['node']['closed'] == False
                            assert wi['node']['startDate'] == fixture.start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
                            assert wi['node']['endDate'] is None
                            assert int(wi['node']['elapsed']) == 10

            class TestWithSubtasks:

                @pytest.fixture()
                def setup(self, setup):
                    fixture = setup
                    api_helper = fixture.api_helper
                    api_helper.update_work_item_attributes(3, dict(work_item_type='subtask'))
                    yield Fixture(
                        parent=fixture
                    )

                def it_does_not_return_subtask_when_include_subtasks_is_false(self, setup):
                    fixture = setup

                    client = Client(schema)
                    result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project.key))

                    assert result['data']
                    all_work_items = result['data']['project']['workItems']['edges']
                    assert len(all_work_items) == 3
                    for wi in all_work_items:
                        assert not wi['node']['key'] == str(uuid.UUID(fixture.work_items[3]['key']))

                def it_returns_subtask_when_include_subtasks_is_defaulted_or_true(self, setup):
                    fixture = setup

                    query = """
                        query getProjectEpicWorkItems($project_key:String!) {
                            project(key: $project_key) {
                                workItems(
                                    interfaces: [EpicNodeRef, ImplementationCost, DevelopmentProgress],
                                    includeEpics: true,
                                    activeWithinDays: 90
                                    ) {
                                    edges {
                                        node {
                                          id
                                          name
                                          key
                                          displayId
                                          epicName
                                          epicKey
                                          budget
                                          effort
                                          authorCount
                                          duration
                                          closed
                                          startDate
                                          endDate
                                          lastUpdate
                                          elapsed
                                        }
                                    }
                                }
                            }
                        }
                    """
                    client = Client(schema)
                    result = client.execute(query, variable_values=dict(project_key=fixture.project.key))

                    assert result['data']
                    all_work_items = result['data']['project']['workItems']['edges']
                    assert len(all_work_items) == 4
                    assert str(uuid.UUID(fixture.work_items[3]['key'])) in [wi['node']['key'] for wi in all_work_items]

            class TestActiveWithinDaysFilter:
                @pytest.fixture()
                def setup(self, setup):
                    fixture = setup
                    api_helper = fixture.api_helper
                    # Closed 9 days ago
                    api_helper.update_delivery_cycle(1, dict(end_date=datetime.utcnow() - timedelta(days=9)))
                    yield Fixture(
                        parent=fixture
                    )

                def it_returns_work_items_with_open_delivery_cycles_in_last_active_within_days(self, setup):
                    fixture = setup

                    query = """
                        query getProjectEpicWorkItems($project_key:String!) {
                            project(key: $project_key) {
                                workItems(
                                    interfaces: [EpicNodeRef, ImplementationCost, DevelopmentProgress],
                                    includeEpics: true,
                                    activeWithinDays: 8
                                    ) {
                                    edges {
                                        node {
                                          id
                                          name
                                          key
                                          displayId
                                          epicName
                                          epicKey
                                          budget
                                          effort
                                          authorCount
                                          duration
                                          closed
                                          startDate
                                          endDate
                                          lastUpdate
                                          elapsed
                                        }
                                    }
                                }
                            }
                        }
                    """
                    client = Client(schema)
                    result = client.execute(query, variable_values=dict(project_key=fixture.project.key))

                    assert result['data']
                    all_work_items = result['data']['project']['workItems']['edges']
                    assert len(all_work_items) == 3
                    assert str(uuid.UUID(fixture.work_items[1]['key'])) not in [wi['node']['key'] for wi in
                                                                                all_work_items]
                    for wi in all_work_items:
                        # Details for parent epic still are unaffected
                        if wi['node']['key'] == str(fixture.epic.key):
                            assert not wi['node']['epicKey']
                            assert not wi['node']['epicName']
                            assert wi['node']['budget'] == 3.0
                            assert wi['node']['effort'] == 7
                            assert wi['node']['authorCount'] == 2
                            assert wi['node']['duration'] > 0
                            assert wi['node']['closed'] == False
                            assert wi['node']['startDate'] == fixture.start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')
                            assert wi['node']['endDate'] is None
                            assert int(wi['node']['elapsed']) == 10
