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
    class TestProjectWorkItemDeliveryCyclesInterfaces(WorkItemApiImportTest):

        class TestInterfaces:
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

        def it_returns_all_delivery_cycles_for_active_items_if_active_only_is_false(self, setup):
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
            assert len(delivery_cycles) == 5

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

            def it_filters_by_a_multiple_tags(self, setup):
                fixture = setup
                client = Client(schema)
                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project.key, tags=['enhancement', 'feature1']))
                assert 'data' in result
                delivery_cycles = result['data']['project']['workItemDeliveryCycles']['edges']
                assert len(delivery_cycles) == 1
                assert {
                    cycle['node']['name']
                    for cycle in delivery_cycles
                } == {

                    'Issue 4'
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

