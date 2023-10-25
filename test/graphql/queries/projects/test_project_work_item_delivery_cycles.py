# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import pytest
from graphene.test import Client
from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *


class TestProjectWorkItemDeliveryCycles(WorkItemApiImportTest):

    class TestWorkItemDeliveryCycles:
        @pytest.fixture
        def setup(self, setup):
            fixture = setup
            organization = fixture.organization
            project = fixture.project
            work_items_source = fixture.work_items_source
            api_helper = WorkItemImportApiHelper(organization, work_items_source)

            work_items_common = dict(
                is_bug=True,
                is_epic=False,
                parent_id=None,
                work_item_type='issue',
                url='http://foo.com',
                description='foo',
                source_id=str(uuid.uuid4()),
            )

            work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 1',
                    display_id='1000',
                    state='backlog',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    tags=['enhancement'],
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 2',
                    display_id='1001',
                    state='upnext',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    tags=['escaped', 'feature1'],
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 3',
                    display_id='1002',
                    state='upnext',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    tags=['escaped', 'feature2'],
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 4',
                    display_id='1004',
                    state='upnext',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    tags=['enhancement', 'feature1'],
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 5',
                    display_id='1005',
                    state='upnext',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    tags=['feature3'],
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 6',
                    display_id='1006',
                    state='closed',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    tags=['escaped', 'feature1'],
                    **work_items_common
                ),

            ]

            api_helper.import_work_items(
                work_items
            )

            yield Fixture(
                project=project,
                work_items=work_items,
                api_helper=api_helper,
                work_items_common=work_items_common
            )

        def it_returns_all_delivery_cycles_for_a_project(self, setup):
            fixture = setup
            client = Client(schema)
            query = """
                        query getProjectWorkItemDeliveryCycles($project_key:String!) {
                            project(key: $project_key) {
                                workItemDeliveryCycles {
                                    edges { 
                                        node {
                                            name
                                            displayId
                                            
                                        }
                                    }
                                }
                            }
                        }
                    """

            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
            assert 'data' in result
            delivery_cycles = result['data']['project']['workItemDeliveryCycles']['edges']
            assert len(delivery_cycles) == 6

        def it_returns_multiple_delivery_cycles_for_the_same_work_item(self, setup):
            fixture = setup

            fixture.api_helper.update_work_items(
                [
                    # close # 4
                    (4, 'closed', datetime.utcnow() - timedelta(days=3)),
                ]
            )

            # now re-open it so that we have two delivery cycles for this item.

            fixture.api_helper.update_work_items(
                [
                    # close # 4
                    (4, 'upnext', datetime.utcnow() - timedelta(days=2)),
                ]
            )

            client = Client(schema)
            query = """
                        query getProjectWorkItemDeliveryCycles($project_key:String!) {
                            project(key: $project_key) {
                                workItemDeliveryCycles {
                                    edges { 
                                        node {
                                            name
                                            displayId
    
                                        }
                                    }
                                }
                            }
                        }
                    """

            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
            assert 'data' in result
            delivery_cycles = result['data']['project']['workItemDeliveryCycles']['edges']
            assert len(delivery_cycles) == 7

        def it_respects_the_active_only_flag(self, setup):
            fixture = setup

            client = Client(schema)
            query = """
                        query getProjectWorkItemDeliveryCycles($project_key:String!) {
                            project(key: $project_key) {
                                workItemDeliveryCycles(activeOnly: true) {
                                    edges { 
                                        node {
                                            name
                                            displayId
    
                                        }
                                    }
                                }
                            }
                        }
                    """

            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
            assert 'data' in result
            delivery_cycles = result['data']['project']['workItemDeliveryCycles']['edges']
            assert len(delivery_cycles) == 4

        def it_returns_only_the_current_delivery_cycle_for_active_items(self, setup):
            fixture = setup

            fixture.api_helper.update_work_items(
                [
                    # close # 4
                    (4, 'closed', datetime.utcnow() - timedelta(days=3)),
                ]
            )

            # now re-open it so that we have two delivery cycles for this item. when active only is specified, we should
            # only return the second delivery cycle

            fixture.api_helper.update_work_items(
                [
                    # close # 4
                    (4, 'upnext', datetime.utcnow() - timedelta(days=2)),
                ]
            )

            client = Client(schema)
            query = """
                        query getProjectWorkItemDeliveryCycles($project_key:String!) {
                            project(key: $project_key) {
                                workItemDeliveryCycles(activeOnly: true) {
                                    edges { 
                                        node {
                                            name
                                            displayId
    
                                        }
                                    }
                                }
                            }
                        }
                    """

            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
            assert 'data' in result
            delivery_cycles = result['data']['project']['workItemDeliveryCycles']['edges']
            assert len(delivery_cycles) == 4

        def it_returns_all_delivery_cycles_for_work_items_if_active_only_is_false(self, setup):
            fixture = setup

            fixture.api_helper.update_work_items(
                [
                    # close # 4
                    (4, 'closed', datetime.utcnow() - timedelta(days=3)),
                ]
            )

            # now re-open it so that we have two delivery cycles for this item.

            fixture.api_helper.update_work_items(
                [
                    # close # 4
                    (4, 'upnext', datetime.utcnow() - timedelta(days=2)),
                ]
            )

            client = Client(schema)
            query = """
                        query getProjectWorkItemDeliveryCycles($project_key:String!) {
                            project(key: $project_key) {
                                workItemDeliveryCycles(activeOnly: false) {
                                    edges { 
                                        node {
                                            name
                                            displayId
    
                                        }
                                    }
                                }
                            }
                        }
                    """

            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
            assert 'data' in result
            delivery_cycles = result['data']['project']['workItemDeliveryCycles']['edges']
            assert len(delivery_cycles) == 7

        def it_returns_cycle_metrics_for_all_delivery_cycles_closed_before_passed_date(self, setup):
            fixture = setup

            fixture.api_helper.update_work_items(
                [
                    # close # 3
                    (3, 'closed', datetime.utcnow() - timedelta(days=3)),
                    # close # 4
                    (4, 'closed', datetime.utcnow() - timedelta(days=2))
                ]
            )
            before_date = datetime.utcnow() - timedelta(days=3)
            before = before_date.strftime("%Y-%m-%d")

            client = Client(schema)
            query = """
                    query getProjectWorkItemDeliveryCycles($project_key:String!, $before:Date) {
                        project(key: $project_key) {
                            workItemDeliveryCycles(
                                closedWithinDays: 10, 
                                defectsOnly: false, 
                                specsOnly: false, 
                                closedBefore: $before,
                                interfaces: [WorkItemInfo, DeliveryCycleInfo, CycleMetrics, ImplementationCost]) 
                                {
                                edges { 
                                    node {
                                      name
                                      key
                                      displayId
                                      workItemKey
                                      workItemType
                                      isBug
                                      state
                                      startDate
                                      endDate
                                      leadTime
                                      cycleTime
                                      latency
                                      effort
                                      duration
                                      authorCount
                                    }
                                }
                            }
                        }
                    }
                """

            result = client.execute(query, variable_values=dict(project_key=fixture.project.key, before=before))
            assert 'data' in result
            delivery_cycles = result['data']['project']['workItemDeliveryCycles']['edges']
            assert len(delivery_cycles) == 1

        def it_respects_a_false_include_sub_tasks_flag_for_closed_delivery_cycles(self, setup):
            fixture = setup
            fixture.api_helper.update_work_item_attributes(4, dict(updated_at=datetime.utcnow() - timedelta(days=2)))
            fixture.api_helper.update_work_item_attributes(5, dict(work_item_type='subtask',
                                                                   updated_at=datetime.utcnow() - timedelta(days=3)))
            fixture.api_helper.update_delivery_cycle(4, dict(end_date=datetime.utcnow() - timedelta(days=2)))
            fixture.api_helper.update_delivery_cycle(5, dict(end_date=datetime.utcnow() - timedelta(days=3)))

            client = Client(schema)
            query = """
                        query getProjectWorkItemDeliveryCycles($project_key:String!) {
                        project(key: $project_key) {
                            workItemDeliveryCycles(
                                closedWithinDays: 10, 
                                specsOnly: false, 
                                includeSubTasks: false,
                                interfaces: [WorkItemInfo, DeliveryCycleInfo, CycleMetrics, ImplementationCost]) 
                                {
                                edges { 
                                    node {
                                      name
                                      key
                                      displayId
                                      workItemKey
                                      workItemType
                                      isBug
                                      state
                                      startDate
                                      endDate
                                      leadTime
                                      cycleTime
                                      latency
                                      effort
                                      duration
                                      authorCount
                                    }
                                }
                            }
                        }
                    }
                    """

            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
            assert 'data' in result
            delivery_cycles = result['data']['project']['workItemDeliveryCycles']['edges']
            assert len(delivery_cycles) == 1

        def it_respects_a_true_include_sub_tasks_flag_for_closed_delivery_cycles(self, setup):
            fixture = setup
            fixture.api_helper.update_work_item_attributes(4, dict(updated_at=datetime.utcnow() - timedelta(days=2)))
            fixture.api_helper.update_work_item_attributes(5, dict(work_item_type='subtask',
                                                                   updated_at=datetime.utcnow() - timedelta(days=3)))
            fixture.api_helper.update_delivery_cycle(4, dict(end_date=datetime.utcnow() - timedelta(days=2)))
            fixture.api_helper.update_delivery_cycle(5, dict(end_date=datetime.utcnow() - timedelta(days=3)))

            client = Client(schema)
            query = """
                        query getProjectWorkItemDeliveryCycles($project_key:String!) {
                        project(key: $project_key) {
                            workItemDeliveryCycles(
                                closedWithinDays: 10, 
                                includeSubTasks: true
                                specsOnly: false, 
                                interfaces: [WorkItemInfo, DeliveryCycleInfo, CycleMetrics, ImplementationCost]) 
                                {
                                edges { 
                                    node {
                                      name
                                      key
                                      displayId
                                      workItemKey
                                      workItemType
                                      isBug
                                      state
                                      startDate
                                      endDate
                                      leadTime
                                      cycleTime
                                      latency
                                      effort
                                      duration
                                      authorCount
                                    }
                                }
                            }
                        }
                    }
                    """

            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
            assert 'data' in result
            delivery_cycles = result['data']['project']['workItemDeliveryCycles']['edges']
            assert len(delivery_cycles) == 2

        def it_returns_cycle_metrics_for_all_delivery_cycles_closed_before_passed_date_and_updated_after(self, setup):
            fixture = setup

            fixture.api_helper.update_work_items(
                [
                    # close # 3
                    (3, 'closed', datetime.utcnow() - timedelta(days=3))
                ]
            )
            fixture.api_helper.update_work_item_attributes(
                3, dict(updated_at=datetime.utcnow())
            )
            before_date = datetime.utcnow() - timedelta(days=3)
            before = before_date.strftime("%Y-%m-%d")

            client = Client(schema)
            query = """
                    query getProjectWorkItemDeliveryCycles($project_key:String!, $before:Date) {
                        project(key: $project_key) {
                            workItemDeliveryCycles(
                                closedWithinDays: 10, 
                                defectsOnly: false, 
                                specsOnly: false, 
                                closedBefore: $before,
                                interfaces: [WorkItemInfo, DeliveryCycleInfo, CycleMetrics, ImplementationCost]) 
                                {
                                edges { 
                                    node {
                                      name
                                      key
                                      displayId
                                      workItemKey
                                      workItemType
                                      isBug
                                      state
                                      startDate
                                      endDate
                                      leadTime
                                      cycleTime
                                      latency
                                      effort
                                      duration
                                      authorCount
                                    }
                                }
                            }
                        }
                    }
                """

            result = client.execute(query, variable_values=dict(project_key=fixture.project.key, before=before))
            assert 'data' in result
            delivery_cycles = result['data']['project']['workItemDeliveryCycles']['edges']
            assert len(delivery_cycles) == 1

        def it_does_not_include_work_items_moved_from_source(self, setup):
            fixture = setup
            fixture.api_helper.update_work_item_attributes(4, dict(is_moved_from_current_source=True))
            fixture.api_helper.update_delivery_cycle(4, dict(end_date=datetime.utcnow() - timedelta(days=2)))
            fixture.api_helper.update_delivery_cycle(5, dict(end_date=datetime.utcnow() - timedelta(days=3)))

            client = Client(schema)
            query = """
                        query getProjectWorkItemDeliveryCycles($project_key:String!) {
                        project(key: $project_key) {
                            workItemDeliveryCycles(
                                closedWithinDays: 10, 
                                specsOnly: false, 
                                interfaces: [WorkItemInfo, DeliveryCycleInfo, CycleMetrics, ImplementationCost]) 
                                {
                                edges { 
                                    node {
                                      name
                                      key
                                      displayId
                                      workItemKey
                                      workItemType
                                      isBug
                                      state
                                      startDate
                                      endDate
                                      leadTime
                                      cycleTime
                                      latency
                                      effort
                                      duration
                                      authorCount
                                    }
                                }
                            }
                        }
                    }
                    """

            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
            assert 'data' in result
            delivery_cycles = result['data']['project']['workItemDeliveryCycles']['edges']
            assert len(delivery_cycles) == 1

        class TestFilteringByTags:

            @pytest.fixture()
            def setup(self, setup):
                fixture = setup
                query = """
                        query getProjectWorkItemDeliveryCycles($project_key:String!, $tags: [String]!) {
                            project(key: $project_key) {
                                workItemDeliveryCycles(tags: $tags) {
                                    edges { 
                                        node {
                                            name
                                            displayId
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


            def it_returns_all_the_items_when_the_tag_list_is_empty(self, setup):
                fixture = setup
                client = Client(schema)
                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project.key, tags=[]))
                assert 'data' in result
                delivery_cycles = result['data']['project']['workItemDeliveryCycles']['edges']
                assert len(delivery_cycles) == 6

            def it_filters_by_a_single_tag(self, setup):
                fixture = setup
                client = Client(schema)
                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project.key, tags=['enhancement']))
                assert 'data' in result
                delivery_cycles = result['data']['project']['workItemDeliveryCycles']['edges']
                assert len(delivery_cycles) == 2
                assert {
                    cycle['node']['name']
                    for cycle in delivery_cycles
                } == {
                    'Issue 1',
                    'Issue 4'
                }

            def it_returns_items_that_match_any_of_the_provided_tags(self, setup):
                fixture = setup
                client = Client(schema)
                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project.key, tags=['enhancement', 'feature1']))
                assert 'data' in result
                delivery_cycles = result['data']['project']['workItemDeliveryCycles']['edges']
                assert len(delivery_cycles) == 4
                assert {
                    cycle['node']['name']
                    for cycle in delivery_cycles
                } == {

                    'Issue 4',
                    'Issue 1',
                    'Issue 6',
                    'Issue 2'
                }

            def it_returns_multiple_delivery_cycles_after_filtering(self, setup):
                fixture = setup

                fixture.api_helper.update_work_items(
                    [
                        # close # 4
                        (4, 'closed', datetime.utcnow() - timedelta(days=3)),
                    ]
                )

                # now re-open it so that we have two delivery cycles for this item.

                fixture.api_helper.update_work_items(
                    [
                        # close # 4
                        (4, 'upnext', datetime.utcnow() - timedelta(days=2)),
                    ]
                )

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project.key, tags=['feature3']))
                assert 'data' in result
                delivery_cycles = result['data']['project']['workItemDeliveryCycles']['edges']
                assert len(delivery_cycles) == 2

                assert {
                           cycle['node']['name']
                           for cycle in delivery_cycles
                       } == {

                           'Issue 5'
                       }

        class TestFilteringByReleases(WorkItemApiImportTest):
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
                                    workItemDeliveryCycles(release: $release){
                                        edges {
                                            node {
                                              id
                                              workItemKey
                                              name
                                              
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

                def it_filters_delivery_cycles_by_a_single_release(self, setup):
                    fixture = setup

                    client = Client(schema)
                    result = client.execute(fixture.query,
                                            variable_values=dict(project_key=fixture.project.key, release='1.0.1'))
                    assert not result.get('errors')
                    assert result['data']
                    edges = result['data']['project']['workItemDeliveryCycles']['edges']
                    assert len(edges) == 2

                    assert {
                               edge['node']['name']
                               for edge in edges
                           } == {
                               'Issue 1',
                               'Issue 3'
                           }


                def it_returns_all_delivery_cycles_if_releases_is_empty(self, setup):
                    fixture = setup

                    client = Client(schema)
                    result = client.execute(fixture.query,
                                            variable_values=dict(project_key=fixture.project.key, release=None))
                    assert not result.get('errors')
                    assert result['data']
                    edges = result['data']['project']['workItemDeliveryCycles']['edges']
                    assert len(edges) == 6


                def it_returns_no_delivery_cycles_if_there_are_no_matches(self, setup):
                    fixture = setup

                    client = Client(schema)
                    result = client.execute(fixture.query,
                                            variable_values=dict(project_key=fixture.project.key,
                                                                 release='random string'))
                    assert not result.get('errors')
                    assert result['data']
                    edges = result['data']['project']['workItemDeliveryCycles']['edges']
                    assert len(edges) == 0

