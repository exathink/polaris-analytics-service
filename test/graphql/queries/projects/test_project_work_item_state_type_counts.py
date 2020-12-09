# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from graphene.test import Client

from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *


class TestProjectWorkItemStateTypeAggregateMetrics:

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
                    query getProjectWorkItemStateTypeAggregateMetrics($project_key:String!) {
                        project(key: $project_key, interfaces: [WorkItemStateTypeAggregateMetrics]) {
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
        work_items_common.pop('parent_id', None)
        work_items_common['parent_key'] = None
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
                            query getProjectWorkItemStateTypeAggregateMetrics($project_key:String!) {
                                project(key: $project_key, interfaces: [WorkItemStateTypeAggregateMetrics]) {
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

    def it_supports_filtering_by_defects_only(self, api_work_items_import_fixture):
        organization, project, work_items_source, _ = api_work_items_import_fixture

        work_items_common = dict(
            work_item_type='issue',
            url='http://foo.com',
            tags=['ares2'],
            description='foo',
            source_id=str(uuid.uuid4()),
            is_epic=False,
            parent_id=None
        )

        api.import_new_work_items(
            organization_key=organization.key,
            work_item_source_key=work_items_source.key,
            work_item_summaries=[
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 1',
                    display_id='1000',
                    state='backlog',
                    is_bug=True,
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 2',
                    display_id='1001',
                    state='upnext',
                    is_bug=False,
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
                    is_bug=True,
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
                    is_bug=False,
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 6',
                    display_id='1006',
                    is_bug=True,
                    state='closed',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),

            ]
        )

        client = Client(schema)
        query = """
                    query getProjectWorkItemStateTypeAggregateMetrics($project_key:String!) {
                        project(key: $project_key, interfaces: [WorkItemStateTypeAggregateMetrics], defectsOnly: true) {
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
        assert state_type_counts['open'] is None
        assert state_type_counts['wip'] == 1
        assert state_type_counts['closed'] == 1
        assert state_type_counts['complete'] is None

    def it_supports_filtering_by_defects_only_when_there_are_no_defects(self, api_work_items_import_fixture):
        organization, project, work_items_source, _ = api_work_items_import_fixture

        work_items_common = dict(
            work_item_type='issue',
            url='http://foo.com',
            tags=['ares2'],
            description='foo',
            source_id=str(uuid.uuid4()),
            is_bug=False
        )

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
                    query getProjectWorkItemStateTypeAggregateMetrics($project_key:String!) {
                        project(key: $project_key, interfaces: [WorkItemStateTypeAggregateMetrics], defectsOnly: true) {
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
        assert state_type_counts['backlog'] is None
        assert state_type_counts['open'] is None
        assert state_type_counts['wip'] is None
        assert state_type_counts['closed'] is None
        assert state_type_counts['complete'] is None

    class TestParameter:

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
                    state='upnext',
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
                api_helper=api_helper,

            )

        class TestClosedWithinDays:
            @pytest.yield_fixture
            def setup(self, setup):
                fixture = setup


                query = """
                        query getProjectWorkItemStateTypeAggregateMetrics($project_key:String!, $closed_within_days: Int!) {
                            project(key: $project_key, interfaces: [WorkItemStateTypeAggregateMetrics], closedWithinDays: $closed_within_days){
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

                yield Fixture(
                    parent=fixture,
                    query=query
            )

            def it_includes_work_items_closed_in_the_closed_within_days_window(self, setup):
                fixture = setup

                closed_within_days_window = 10

                fixture.api_helper.update_work_items(
                    [
                        # close the third work item within the test window
                        (3, 'closed', datetime.utcnow() - timedelta(closed_within_days_window - 1)),
                    ]
                )

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    closed_within_days=closed_within_days_window
                ))
                assert 'data' in result
                state_type_counts = result['data']['project']['workItemStateTypeCounts']
                assert state_type_counts['backlog'] == 1
                assert state_type_counts['open'] == 4
                assert state_type_counts['wip'] is None
                assert state_type_counts['closed'] == 1
                assert state_type_counts['complete'] is None

            def it_excludes_work_items_closed_in_the_closed_within_days_window(self, setup):
                fixture = setup

                closed_within_days_window = 10

                fixture.api_helper.update_work_items(
                    [
                        # close the third work item within the test window
                        (3, 'closed', datetime.utcnow() - timedelta(closed_within_days_window - 1)),
                        # close the fourth work item outside the test window
                        (4, 'closed', datetime.utcnow() - timedelta(closed_within_days_window + 1)),
                    ]
                )

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    closed_within_days=closed_within_days_window
                ))
                assert 'data' in result
                state_type_counts = result['data']['project']['workItemStateTypeCounts']
                assert state_type_counts['backlog'] == 1
                assert state_type_counts['open'] == 3
                assert state_type_counts['wip'] is None
                assert state_type_counts['closed'] == 1
                assert state_type_counts['complete'] is None

            def it_returns_none_when_all_work_items_closed_are_outside_the_closed_within_days_window(self, setup):
                fixture = setup

                closed_within_days_window = 10

                fixture.api_helper.update_work_items(
                    [
                        # close the third work item outside the test window
                        (3, 'closed', datetime.utcnow() - timedelta(closed_within_days_window + 1)),
                    ]
                )

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    closed_within_days=closed_within_days_window
                ))
                assert 'data' in result
                state_type_counts = result['data']['project']['workItemStateTypeCounts']
                assert state_type_counts['backlog'] == 1
                assert state_type_counts['open'] == 4
                assert state_type_counts['wip'] is None
                assert state_type_counts['closed'] == None
                assert state_type_counts['complete'] is None

            def it_reports_closed_work_items_that_are_reopened_correctly(self, setup):
                fixture = setup

                closed_within_days_window = 10

                fixture.api_helper.update_work_items(
                    [
                        # close the third work item inside the test window
                        (3, 'closed', datetime.utcnow() - timedelta(closed_within_days_window - 1)),
                    ]
                )
                fixture.api_helper.update_work_items(
                    [
                        # reopen third work item
                        (3, 'upnext', datetime.utcnow() - timedelta(closed_within_days_window - 2)),
                    ]
                )

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    closed_within_days=closed_within_days_window
                ))
                assert 'data' in result
                state_type_counts = result['data']['project']['workItemStateTypeCounts']
                assert state_type_counts['backlog'] == 1
                assert state_type_counts['open'] == 5  # include the current delivery cycle of the re-opened item
                assert state_type_counts['wip'] is None
                assert state_type_counts['closed'] == 1  # prior delivery cycle of the closed item
                assert state_type_counts['complete'] is None

            def it_reports_closed_work_items_that_are_closed_multiple_times_correctly(self, setup):
                fixture = setup

                closed_within_days_window = 10

                fixture.api_helper.update_work_items(
                    [
                        # close the third work item inside the test window
                        (3, 'closed', datetime.utcnow() - timedelta(closed_within_days_window - 1)),
                    ]
                )
                fixture.api_helper.update_work_items(
                    [
                        # reopen third work item
                        (3, 'upnext', datetime.utcnow() - timedelta(closed_within_days_window - 2)),
                    ]
                )

                fixture.api_helper.update_work_items(
                    [
                        # close it again
                        (3, 'closed', datetime.utcnow() - timedelta(closed_within_days_window - 3)),
                    ]
                )

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    closed_within_days=closed_within_days_window
                ))
                assert 'data' in result
                state_type_counts = result['data']['project']['workItemStateTypeCounts']
                assert state_type_counts['backlog'] == 1
                assert state_type_counts['open'] == 4
                assert state_type_counts['wip'] is None
                assert state_type_counts['closed'] == 2  # prior delivery cycle of the closed item
                assert state_type_counts['complete'] is None

        class TestSpecsOnly:
            @pytest.yield_fixture
            def setup(self, setup):
                fixture = setup


                query = """
                        query getProjectWorkItemStateTypeAggregateMetrics($project_key:String!) {
                            project(key: $project_key, interfaces: [WorkItemStateTypeAggregateMetrics], specsOnly: true){
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

                yield Fixture(
                    parent=fixture,
                    query=query
            )

            def it_respects_the_specs_only_parameter(self, setup):
                fixture = setup

                fixture.api_helper.update_delivery_cycles(
                    [
                        # make # 4 and #5 specs
                        (4, dict(property='commit_count', value=2)),
                        (5, dict(property='commit_count', value=2))
                    ]
                )

                fixture.api_helper.update_work_items(
                    [
                        # close # 4 so we have closed and open specs.
                        (4, 'closed', datetime.utcnow()),
                    ]
                )

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                ))
                assert 'data' in result
                state_type_counts = result['data']['project']['workItemStateTypeCounts']
                assert state_type_counts['backlog'] is None
                assert state_type_counts['open'] == 1
                assert state_type_counts['wip'] is None
                assert state_type_counts['closed'] == 1
                assert state_type_counts['complete'] is None


class TestProjectTotalEffortByStateType:

    def it_returns_total_effort_counts_when_there_are_specs_in_the_project(self,
                                                                           api_work_items_import_fixture):
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
        api_helper.import_work_items(
            work_items
        )

        api_helper.update_delivery_cycles([(3, dict(property='commit_count', value=2)), (5, dict(property='commit_count', value=1))])
        api_helper.update_delivery_cycles(
            [(3, dict(property='effort', value=1)), (5, dict(property='effort', value=2))])

        client = Client(schema)
        query = """
                    query getProjectWorkItemStateTypeAggregateMetrics($project_key:String!) {
                        project(key: $project_key, interfaces: [WorkItemStateTypeAggregateMetrics]) {
                            totalEffortByStateType {
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
        state_type_counts = result['data']['project']['totalEffortByStateType']
        assert state_type_counts['backlog'] == 0
        assert state_type_counts['open'] == 0
        assert state_type_counts['wip'] == 1
        assert state_type_counts['closed'] == 2
        assert state_type_counts['complete'] is None
