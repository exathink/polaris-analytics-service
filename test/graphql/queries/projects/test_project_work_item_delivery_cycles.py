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


class TestProjectWorkItemDeliveryCycles:

    @pytest.yield_fixture
    def setup(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        work_items = [
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
                state='upnext',
                created_at=get_date("2018-12-02"),
                updated_at=get_date("2018-12-03"),
                **work_items_common
            ),
            dict(
                key=uuid.uuid4().hex,
                name='Issue 5',
                display_id='1005',
                state='upnext',
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
        before = before_date.strftime("%Y-%m-%dT00:00:00.000Z")

        client = Client(schema)
        query = """
                query getProjectWorkItemDeliveryCycles($project_key:String!, $before:DateTime) {
                    project(key: $project_key) {
                        workItemDeliveryCycles(
                            closedWithinDays: 10, 
                            defectsOnly: false, 
                            specsOnly: true, 
                            before: $before,
                            interfaces: [WorkItemInfo, DeliveryCycleInfo, CycleMetrics, ImplementationCost]) 
                            {
                            edges { 
                                node {
                                    name
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
