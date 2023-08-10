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
from polaris.utils.collections import dict_merge

class TestProjectFunnelViewAggregateMetrics(WorkItemApiImportTest):

    class TestFunnelViewAggrgateMetrics:
        @pytest.fixture
        def setup(self, setup):
            fixture = setup

            project = fixture.project
            api_helper = fixture.api_helper
            work_items_common_fields = fixture.work_items_common

            work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 1',
                    display_id='1001',
                    state='backlog',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **dict_merge(
                        work_items_common_fields,
                        dict(is_bug=False, work_item_type='issue', tags=['enhancements'], releases=['1.0.1'])
                    )
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 2',
                    display_id='1002',
                    state='upnext',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **dict_merge(
                        work_items_common_fields,
                        dict(is_bug=False, work_item_type='issue', tags=['enhancements', 'feature1'], releases=['1.0.1'])
                    )
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 3',
                    display_id='1003',
                    state='doing',
                    effort=1,
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **dict_merge(
                        work_items_common_fields,
                        dict(is_bug=False, work_item_type='issue', tags=['enhancements', 'feature2'], releases=['1.0.1'])
                    )
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 4',
                    display_id='1004',
                    state='closed',
                    effort=2,
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **dict_merge(
                        work_items_common_fields,
                        dict(is_bug=False, work_item_type='issue', tags=['enhancements', 'feature1'], releases=['1.0.1'])
                    )
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Bug 1',
                    display_id='1005',
                    state='backlog',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **dict_merge(
                        work_items_common_fields,
                        dict(is_bug=True, work_item_type='bug', tags=['feature2'], releases=['1.0.2'])
                    )
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Bug 2',
                    display_id='1006',
                    state='upnext',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **dict_merge(
                        work_items_common_fields,
                        dict(is_bug=True, work_item_type='bug')
                    )
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Bug 3',
                    display_id='1007',
                    state='doing',
                    effort=1,
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **dict_merge(
                        work_items_common_fields,
                        dict(is_bug=True, work_item_type='bug')
                    )
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Bug 4',
                    display_id='1008',
                    state='closed',
                    effort=2,
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **dict_merge(
                        work_items_common_fields,
                        dict(is_bug=True, work_item_type='bug')
                    )
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Subtask 1',
                    display_id='1009',
                    state='backlog',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **dict_merge(
                        work_items_common_fields,
                        dict(is_bug=False, work_item_type='subtask')
                    )
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Subtask 2',
                    display_id='1010',
                    state='upnext',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **dict_merge(
                        work_items_common_fields,
                        dict(is_bug=False, work_item_type='subtask')
                    )
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Subtask 3',
                    display_id='1011',
                    state='doing',
                    effort=1,
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **dict_merge(
                        work_items_common_fields,
                        dict(is_bug=False, work_item_type='subtask')
                    )
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Subtask 4',
                    display_id='1012',
                    state='closed',
                    effort=2,
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **dict_merge(
                        work_items_common_fields,
                        dict(is_bug=False, work_item_type='subtask')
                    )
                ),

            ]

            api_helper.import_work_items(
                work_items
            )

            api_helper.update_delivery_cycle(3, dict(end_date=datetime.utcnow(), effort=2))
            api_helper.update_delivery_cycle(7, dict(end_date=datetime.utcnow(), effort=2))
            api_helper.update_delivery_cycle(11, dict(end_date=datetime.utcnow(), effort=2))
            api_helper.update_delivery_cycle(2, dict(effort=1))
            api_helper.update_delivery_cycle(6, dict(effort=1))
            api_helper.update_delivery_cycle(10, dict(effort=1))

            yield Fixture(
                parent=fixture,
                work_items=work_items
            )

        def it_returns_work_items_in_all_state_types_including_subtasks_for_both(self, setup):
            fixture = setup
            client = Client(schema)
            query = """
                    query getProjectWorkItemsStateTypeAggregates($project_key:String!) {
                        project(
                            key: $project_key,
                            interfaces: [FunnelViewAggregateMetrics],
                            specsOnly: false,
                            closedWithinDays: 30
                            funnelViewArgs: {
                                includeSubTasksInClosedState: true
                                includeSubTasksInNonClosedState: true
                            }
                            )
                            {
                                workItemStateTypeCounts {
                                  backlog
                                  open
                                  wip
                                  complete
                                  closed
                                  unmapped
                                }
                                totalEffortByStateType {
                                  backlog
                                  open
                                  wip
                                  complete
                                  closed
                                  unmapped
                            }
                        }
                    }
                """

            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
            assert 'data' in result
            work_item_state_type_counts = result['data']['project']['workItemStateTypeCounts']
            total_effort_by_state = result['data']['project']['totalEffortByStateType']
            assert work_item_state_type_counts['backlog'] == 3
            assert work_item_state_type_counts['open'] == 3
            assert work_item_state_type_counts['wip'] == 3
            assert work_item_state_type_counts['complete'] == None
            assert work_item_state_type_counts['closed'] == 3
            assert work_item_state_type_counts['unmapped'] == None
            assert total_effort_by_state['backlog'] == 0
            assert total_effort_by_state['open'] == 0
            assert total_effort_by_state['wip'] == 3
            assert total_effort_by_state['complete'] == None
            assert total_effort_by_state['closed'] == 6
            assert total_effort_by_state['unmapped'] == None

        def it_returns_work_items_in_all_state_types_including_subtasks_only_in_non_closed_states(self, setup):
            fixture = setup
            client = Client(schema)
            query = """
                    query getProjectWorkItemsStateTypeAggregates($project_key:String!) {
                        project(
                            key: $project_key,
                            interfaces: [FunnelViewAggregateMetrics],
                            specsOnly: false,
                            closedWithinDays: 30
                            funnelViewArgs: {
                              includeSubTasksInClosedState: false
                              includeSubTasksInNonClosedState: true
                            }
                            )
                            {
                                workItemStateTypeCounts {
                                  backlog
                                  open
                                  wip
                                  complete
                                  closed
                                  unmapped
                                }
                                totalEffortByStateType {
                                  backlog
                                  open
                                  wip
                                  complete
                                  closed
                                  unmapped
                            }
                        }
                    }
                """

            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
            assert 'data' in result
            work_item_state_type_counts = result['data']['project']['workItemStateTypeCounts']
            total_effort_by_state = result['data']['project']['totalEffortByStateType']
            assert work_item_state_type_counts['backlog'] == 3
            assert work_item_state_type_counts['open'] == 3
            assert work_item_state_type_counts['wip'] == 3
            assert work_item_state_type_counts['complete'] == None
            assert work_item_state_type_counts['closed'] == 2
            assert work_item_state_type_counts['unmapped'] == None
            assert total_effort_by_state['backlog'] == 0
            assert total_effort_by_state['open'] == 0
            assert total_effort_by_state['wip'] == 3
            assert total_effort_by_state['complete'] == None
            assert total_effort_by_state['closed'] == 4
            assert total_effort_by_state['unmapped'] == None

        def it_returns_work_items_in_all_state_types_including_subtasks_only_in_closed_states(self, setup):
            fixture = setup
            client = Client(schema)
            query = """
                    query getProjectWorkItemsStateTypeAggregates($project_key:String!) {
                        project(
                            key: $project_key,
                            interfaces: [FunnelViewAggregateMetrics],
                            specsOnly: false,
                            closedWithinDays: 30
                            funnelViewArgs: {
                              includeSubTasksInClosedState: true
                              includeSubTasksInNonClosedState: false
                            }
                            )
                            {
                                workItemStateTypeCounts {
                                  backlog
                                  open
                                  wip
                                  complete
                                  closed
                                  unmapped
                                }
                                totalEffortByStateType {
                                  backlog
                                  open
                                  wip
                                  complete
                                  closed
                                  unmapped
                            }
                        }
                    }
                """

            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
            assert 'data' in result
            work_item_state_type_counts = result['data']['project']['workItemStateTypeCounts']
            total_effort_by_state = result['data']['project']['totalEffortByStateType']
            assert work_item_state_type_counts['backlog'] == 2
            assert work_item_state_type_counts['open'] == 2
            assert work_item_state_type_counts['wip'] == 2
            assert work_item_state_type_counts['complete'] == None
            assert work_item_state_type_counts['closed'] == 3
            assert work_item_state_type_counts['unmapped'] == None
            assert total_effort_by_state['backlog'] == 0
            assert total_effort_by_state['open'] == 0
            assert total_effort_by_state['wip'] == 2
            assert total_effort_by_state['complete'] == None
            assert total_effort_by_state['closed'] == 6
            assert total_effort_by_state['unmapped'] == None

        def it_returns_work_items_in_all_state_types_excluding_subtasks_for_both(self, setup):
            fixture = setup
            client = Client(schema)
            query = """
                    query getProjectWorkItemsStateTypeAggregates($project_key:String!) {
                        project(
                            key: $project_key,
                            interfaces: [FunnelViewAggregateMetrics],
                            specsOnly: false,
                            closedWithinDays: 30
                            funnelViewArgs: {
                              includeSubTasksInClosedState: false
                              includeSubTasksInNonClosedState: false
                            }
                            )
                            {
                                workItemStateTypeCounts {
                                  backlog
                                  open
                                  wip
                                  complete
                                  closed
                                  unmapped
                                }
                                totalEffortByStateType {
                                  backlog
                                  open
                                  wip
                                  complete
                                  closed
                                  unmapped
                            }
                        }
                    }
                """

            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
            assert 'data' in result
            work_item_state_type_counts = result['data']['project']['workItemStateTypeCounts']
            total_effort_by_state = result['data']['project']['totalEffortByStateType']
            assert work_item_state_type_counts['backlog'] == 2
            assert work_item_state_type_counts['open'] == 2
            assert work_item_state_type_counts['wip'] == 2
            assert work_item_state_type_counts['complete'] == None
            assert work_item_state_type_counts['closed'] == 2
            assert work_item_state_type_counts['unmapped'] == None
            assert total_effort_by_state['backlog'] == 0
            assert total_effort_by_state['open'] == 0
            assert total_effort_by_state['wip'] == 2
            assert total_effort_by_state['complete'] == None
            assert total_effort_by_state['closed'] == 4
            assert total_effort_by_state['unmapped'] == None

        def it_returns_correct_counts_in_case_of_multiple_delivery_cycles(self, setup):
            fixture = setup
            with db.orm_session() as session:
                reopened_work_item = WorkItem.find_by_work_item_key(session, fixture.work_items[3]['key'])
                new_delivery_cycle = WorkItemDeliveryCycle(
                    start_seq_no=0,
                    start_date=datetime.utcnow(),
                    work_item_id=reopened_work_item.id,
                    work_items_source_id=reopened_work_item.work_items_source_id
                )
                session.add(new_delivery_cycle)
                session.flush()
                reopened_work_item.current_delivery_cycle_id = new_delivery_cycle.delivery_cycle_id
                reopened_work_item.state_type = 'backlog'
                reopened_work_item.state = 'backlog'

            client = Client(schema)
            query = """
                    query getProjectWorkItemsStateTypeAggregates($project_key:String!) {
                        project(
                            key: $project_key,
                            interfaces: [FunnelViewAggregateMetrics],
                            specsOnly: false,
                            closedWithinDays: 30
                            funnelViewArgs: {
                              includeSubTasksInClosedState: true
                              includeSubTasksInNonClosedState: true
                            }
                            )
                            {
                                workItemStateTypeCounts {
                                  backlog
                                  open
                                  wip
                                  complete
                                  closed
                                  unmapped
                                }
                                totalEffortByStateType {
                                  backlog
                                  open
                                  wip
                                  complete
                                  closed
                                  unmapped
                            }
                        }
                    }
                """

            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
            assert 'data' in result
            work_item_state_type_counts = result['data']['project']['workItemStateTypeCounts']
            total_effort_by_state = result['data']['project']['totalEffortByStateType']
            assert work_item_state_type_counts['backlog'] == 4
            assert work_item_state_type_counts['open'] == 3
            assert work_item_state_type_counts['wip'] == 3
            assert work_item_state_type_counts['complete'] == None
            assert work_item_state_type_counts['closed'] == 3
            assert work_item_state_type_counts['unmapped'] == None
            assert total_effort_by_state['backlog'] == 0
            assert total_effort_by_state['open'] == 0
            assert total_effort_by_state['wip'] == 3
            assert total_effort_by_state['complete'] == None
            assert total_effort_by_state['closed'] == 6
            assert total_effort_by_state['unmapped'] == None

        def it_returns_correct_counts_in_case_of_item_in_unmapped_state(self, setup):
            fixture = setup
            api_helper = fixture.api_helper
            # Change an item from wip to an unmapped state
            api_helper.update_work_item(2, dict(state='Ready For Prod', state_type=None))

            client = Client(schema)
            query = """
                    query getProjectWorkItemsStateTypeAggregates($project_key:String!) {
                        project(
                            key: $project_key,
                            interfaces: [FunnelViewAggregateMetrics],
                            specsOnly: false,
                            closedWithinDays: 30
                            funnelViewArgs: {
                              includeSubTasksInClosedState: true
                              includeSubTasksInNonClosedState: true
                            }
                            )
                            {
                                workItemStateTypeCounts {
                                  backlog
                                  open
                                  wip
                                  complete
                                  closed
                                  unmapped
                                }
                                totalEffortByStateType {
                                  backlog
                                  open
                                  wip
                                  complete
                                  closed
                                  unmapped
                            }
                        }
                    }
                """

            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
            assert 'data' in result
            work_item_state_type_counts = result['data']['project']['workItemStateTypeCounts']
            total_effort_by_state = result['data']['project']['totalEffortByStateType']
            assert work_item_state_type_counts['backlog'] == 3
            assert work_item_state_type_counts['open'] == 3
            assert work_item_state_type_counts['wip'] == 2
            assert work_item_state_type_counts['complete'] == None
            assert work_item_state_type_counts['closed'] == 3
            assert work_item_state_type_counts['unmapped'] == 1
            assert total_effort_by_state['backlog'] == 0
            assert total_effort_by_state['open'] == 0
            assert total_effort_by_state['wip'] == 2
            assert total_effort_by_state['complete'] == None
            assert total_effort_by_state['closed'] == 6
            assert total_effort_by_state['unmapped'] == 1

        def it_excludes_deferred_items_in_any_phase(self, setup):
            fixture = setup
            api_helper = fixture.api_helper

            # Change all the states to deferred
            with db.orm_session() as session:
                session.add(fixture.work_items_source)
                for state_map in fixture.work_items_source.state_maps:
                    state_map.release_status = WorkItemsStateReleaseStatusType.deferred.value

            client = Client(schema)
            query = """
                    query getProjectWorkItemsStateTypeAggregates($project_key:String!) {
                        project(
                            key: $project_key,
                            interfaces: [FunnelViewAggregateMetrics],
                            specsOnly: false,
                            closedWithinDays: 30
                            funnelViewArgs: {
                              includeSubTasksInClosedState: true
                              includeSubTasksInNonClosedState: true
                            }
                            )
                            {
                                workItemStateTypeCounts {
                                  backlog
                                  open
                                  wip
                                  complete
                                  closed
                                  unmapped
                                }
                                totalEffortByStateType {
                                  backlog
                                  open
                                  wip
                                  complete
                                  closed
                                  unmapped
                            }
                        }
                    }
                """

            result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
            assert 'data' in result
            work_item_state_type_counts = result['data']['project']['workItemStateTypeCounts']
            total_effort_by_state = result['data']['project']['totalEffortByStateType']
            assert work_item_state_type_counts['backlog'] is None
            assert work_item_state_type_counts['open'] is None
            assert work_item_state_type_counts['wip'] is None
            assert work_item_state_type_counts['complete'] is None
            assert work_item_state_type_counts['closed'] is None
            assert work_item_state_type_counts['unmapped'] is None
            assert total_effort_by_state['backlog'] is None
            assert total_effort_by_state['open'] is None
            assert total_effort_by_state['wip'] is None
            assert total_effort_by_state['complete'] is None
            assert total_effort_by_state['closed'] is None

        class TestFilterWorkItemsByTag:

            @pytest.fixture()
            def setup(self, setup):
                fixture = setup
                query = """
                            query getProjectWorkItemsStateTypeAggregates($project_key:String!, $tags:[String]!) {
                                project(
                                    key: $project_key,
                                    interfaces: [FunnelViewAggregateMetrics],
                                    specsOnly: false,
                                    closedWithinDays: 30,
                                    tags: $tags,
                                    funnelViewArgs: {
                                        includeSubTasksInClosedState: true
                                        includeSubTasksInNonClosedState: true
                                    }
                                    )
                                    {
                                        workItemStateTypeCounts {
                                          backlog
                                          open
                                          wip
                                          complete
                                          closed
                                          unmapped
                                        }
                                        totalEffortByStateType {
                                          backlog
                                          open
                                          wip
                                          complete
                                          closed
                                          unmapped
                                    }
                                }
                            }
                        """
                yield Fixture(
                    parent=fixture,
                    query=query
                )

            def it_returns_all_work_items_when_tags_are_not_specified(self, setup):
                fixture = setup
                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project.key, tags=[]))
                assert 'data' in result
                work_item_state_type_counts = result['data']['project']['workItemStateTypeCounts']
                total_effort_by_state = result['data']['project']['totalEffortByStateType']
                assert work_item_state_type_counts['backlog'] == 3
                assert work_item_state_type_counts['open'] == 3
                assert work_item_state_type_counts['wip'] == 3
                assert work_item_state_type_counts['complete'] == None
                assert work_item_state_type_counts['closed'] == 3
                assert work_item_state_type_counts['unmapped'] == None
                assert total_effort_by_state['backlog'] == 0
                assert total_effort_by_state['open'] == 0
                assert total_effort_by_state['wip'] == 3
                assert total_effort_by_state['complete'] == None
                assert total_effort_by_state['closed'] == 6
                assert total_effort_by_state['unmapped'] == None

            def it_filters_work_items_when_tags_are_specified(self, setup):
                fixture = setup
                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project.key, tags=['enhancements']))
                assert 'data' in result
                work_item_state_type_counts = result['data']['project']['workItemStateTypeCounts']
                total_effort_by_state = result['data']['project']['totalEffortByStateType']
                assert work_item_state_type_counts['backlog'] == 1
                assert work_item_state_type_counts['open'] == 1
                assert work_item_state_type_counts['wip'] == 1
                assert work_item_state_type_counts['complete'] == None
                assert work_item_state_type_counts['closed'] == 1
                assert work_item_state_type_counts['unmapped'] == None
                assert total_effort_by_state['backlog'] == 0
                assert total_effort_by_state['open'] == 0
                assert total_effort_by_state['wip'] == 1
                assert total_effort_by_state['complete'] == None
                assert total_effort_by_state['closed'] == 2
                assert total_effort_by_state['unmapped'] == None

        class TestFilterWorkItemsByRelease:

            @pytest.fixture()
            def setup(self, setup):
                fixture = setup
                query = """
                            query getProjectWorkItemsStateTypeAggregates($project_key:String!, $release:String) {
                                project(
                                    key: $project_key,
                                    interfaces: [FunnelViewAggregateMetrics],
                                    specsOnly: false,
                                    closedWithinDays: 30,
                                    release: $release,
                                    funnelViewArgs: {
                                        includeSubTasksInClosedState: true
                                        includeSubTasksInNonClosedState: true
                                    }
                                    )
                                    {
                                        workItemStateTypeCounts {
                                          backlog
                                          open
                                          wip
                                          complete
                                          closed
                                          unmapped
                                        }
                                        totalEffortByStateType {
                                          backlog
                                          open
                                          wip
                                          complete
                                          closed
                                          unmapped
                                    }
                                }
                            }
                        """
                yield Fixture(
                    parent=fixture,
                    query=query
                )

            def it_returns_all_work_items_when_release_is_not_specified(self, setup):
                fixture = setup
                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project.key, release=None))
                assert 'data' in result
                work_item_state_type_counts = result['data']['project']['workItemStateTypeCounts']
                total_effort_by_state = result['data']['project']['totalEffortByStateType']
                assert work_item_state_type_counts['backlog'] == 3
                assert work_item_state_type_counts['open'] == 3
                assert work_item_state_type_counts['wip'] == 3
                assert work_item_state_type_counts['complete'] == None
                assert work_item_state_type_counts['closed'] == 3
                assert work_item_state_type_counts['unmapped'] == None
                assert total_effort_by_state['backlog'] == 0
                assert total_effort_by_state['open'] == 0
                assert total_effort_by_state['wip'] == 3
                assert total_effort_by_state['complete'] == None
                assert total_effort_by_state['closed'] == 6
                assert total_effort_by_state['unmapped'] == None

            def it_filters_work_items_when_release_is_specified(self, setup):
                fixture = setup
                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project.key, release='1.0.1'))
                assert 'data' in result
                work_item_state_type_counts = result['data']['project']['workItemStateTypeCounts']
                total_effort_by_state = result['data']['project']['totalEffortByStateType']
                assert work_item_state_type_counts['backlog'] == 1
                assert work_item_state_type_counts['open'] == 1
                assert work_item_state_type_counts['wip'] == 1
                assert work_item_state_type_counts['complete'] == None
                assert work_item_state_type_counts['closed'] == 1
                assert work_item_state_type_counts['unmapped'] == None
                assert total_effort_by_state['backlog'] == 0
                assert total_effort_by_state['open'] == 0
                assert total_effort_by_state['wip'] == 1
                assert total_effort_by_state['complete'] == None
                assert total_effort_by_state['closed'] == 2
                assert total_effort_by_state['unmapped'] == None
