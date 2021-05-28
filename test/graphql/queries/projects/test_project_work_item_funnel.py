# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from graphene.test import Client

from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *
from polaris.utils.collections import dict_merge


# This tests the aggregate counts in the funnel view
class TestProjectFunnelViewAggregateMetrics:

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
                    query getProjectFunnelViewAggregateMetrics($project_key:String!) {
                        project(key: $project_key, interfaces: [FunnelViewAggregateMetrics]) {
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
                            query getProjectFunnelViewAggregateMetrics($project_key:String!) {
                                project(key: $project_key, interfaces: [FunnelViewAggregateMetrics]) {
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
                    query getProjectFunnelViewAggregateMetrics($project_key:String!) {
                        project(key: $project_key, interfaces: [FunnelViewAggregateMetrics], defectsOnly: true) {
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

    def it_excludes_epics_by_default(self, api_work_items_import_fixture):
        organization, project, work_items_source, _ = api_work_items_import_fixture

        work_items_common = dict(
            work_item_type='issue',
            url='http://foo.com',
            tags=['ares2'],
            description='foo',
            source_id=str(uuid.uuid4()),
            is_bug=False,
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
                    is_epic=False,
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 2',
                    display_id='1001',
                    state='upnext',
                    is_epic=False,
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 3',
                    display_id='1002',
                    state='upnext',
                    is_epic=True,
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 4',
                    display_id='1004',
                    is_epic=False,
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
                    is_epic=True,
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 6',
                    display_id='1006',
                    is_epic=True,
                    state='closed',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Issue 6',
                    display_id='1006',
                    is_epic=False,
                    state='closed',
                    created_at=get_date("2018-12-02"),
                    updated_at=get_date("2018-12-03"),
                    **work_items_common
                ),

            ]
        )

        client = Client(schema)
        query = """
                    query getProjectFunnelViewAggregateMetrics($project_key:String!) {
                        project(key: $project_key, interfaces: [FunnelViewAggregateMetrics]) {
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
        assert state_type_counts['open'] == 1
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
                    query getProjectFunnelViewAggregateMetrics($project_key:String!) {
                        project(key: $project_key, interfaces: [FunnelViewAggregateMetrics], defectsOnly: true) {
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
                        query getProjectFunnelViewAggregateMetrics($project_key:String!, $closed_within_days: Int!) {
                            project(key: $project_key, interfaces: [FunnelViewAggregateMetrics], closedWithinDays: $closed_within_days){
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
                        query getProjectFunnelViewAggregateMetrics($project_key:String!) {
                            project(key: $project_key, interfaces: [FunnelViewAggregateMetrics], specsOnly: true){
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


# This tests the effort values that show up in the funnel.
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

        api_helper.update_delivery_cycles(
            [(3, dict(property='commit_count', value=2)), (5, dict(property='commit_count', value=1))])
        api_helper.update_delivery_cycles(
            [(3, dict(property='effort', value=1)), (5, dict(property='effort', value=2))])

        client = Client(schema)
        query = """
                    query getProjectFunnelViewAggregateMetrics($project_key:String!) {
                        project(key: $project_key, interfaces: [FunnelViewAggregateMetrics]) {
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


# This tests the detailed work items that are selected in the funnel details view.
class TestProjectWorkItemsFunnelView:

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

    def it_returns_work_items_in_all_state_types(self, setup):
        fixture = setup
        client = Client(schema)
        query = """
                    query getProjectWorkItemsFunnelView($project_key:String!) {
                        project(key: $project_key) {
                            workItems(funnelView: true) {
                                edges { 
                                    node {
                                        name
                                        displayId
                                        state
                                        stateType
                                    }
                                }
                            }
                        }
                    }
                """

        result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
        assert 'data' in result
        work_items = result['data']['project']['workItems']['edges']
        assert len(work_items) == 6

    def it_returns_unmapped_work_items_as_unmapped(self, setup):
        fixture = setup
        fixture.api_helper.import_work_items([
            dict(
                key=uuid.uuid4().hex,
                name='Issue 7',
                display_id='1007',
                state='unknown_state',
                created_at=get_date("2018-12-02"),
                updated_at=get_date("2018-12-03"),
                **fixture.work_items_common
            )
        ]),
        client = Client(schema)
        query = """
                    query getProjectWorkItemsFunnelView($project_key:String!) {
                        project(key: $project_key) {
                            workItems(funnelView: true) {
                                edges { 
                                    node {
                                        name
                                        displayId
                                        state
                                        stateType
                                    }
                                }
                            }
                        }
                    }
                """

        result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
        assert 'data' in result
        work_items = [edge['node'] for edge in result['data']['project']['workItems']['edges']]
        assert len(work_items) == 7
        assert find(work_items, lambda work_item: work_item['name'] == 'Issue 7')['stateType'] == 'unmapped'

    def it_respects_the_closed_within_days_param(self, setup):
        fixture = setup

        fixture.api_helper.update_work_items(
            [
                # close # 4 so we have closed work items within the window.
                (4, 'closed', datetime.utcnow() - timedelta(days=3)),
            ]
        )
        # work item 6 is already closed, but it is outside the window so it should be skipped.

        client = Client(schema)
        query = """
                    query getProjectWorkItemsFunnelView($project_key:String!) {
                        project(key: $project_key) {
                            workItems(funnelView: true, closedWithinDays: 10) {
                                edges { 
                                    node {
                                        name
                                        displayId
                                        state
                                        stateType
                                    }
                                }
                            }
                        }
                    }
                """

        result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
        assert 'data' in result
        work_items = result['data']['project']['workItems']['edges']
        assert len(work_items) == 5

    def it_respects_the_specs_only_param(self, setup):
        fixture = setup

        fixture.api_helper.update_delivery_cycles(
            [
                # make # 4 and #5 specs
                (4, dict(property='commit_count', value=2)),
                (5, dict(property='commit_count', value=2))
            ]
        )

        client = Client(schema)
        query = """
                    query getProjectWorkItemsFunnelView($project_key:String!) {
                        project(key: $project_key) {
                            workItems(funnelView: true, specsOnly: true) {
                                edges { 
                                    node {
                                        name
                                        displayId
                                        state
                                        stateType
                                    }
                                }
                            }
                        }
                    }
                """

        result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
        assert 'data' in result
        work_items = result['data']['project']['workItems']['edges']
        assert len(work_items) == 2

    def it_returns_multiple_closed_delivery_cycles_for_the_same_work_item(self, setup):
        fixture = setup

        fixture.api_helper.update_work_items(
            [
                # close # 4 so we have closed work items within the window.
                (4, 'closed', datetime.utcnow() - timedelta(days=3)),
            ]
        )

        fixture.api_helper.update_work_items(
            [
                # reopen it
                (4, 'doing', datetime.utcnow() - timedelta(days=2)),
            ]
        )

        fixture.api_helper.update_work_items(
            [
                # close it again
                (4, 'closed', datetime.utcnow() - timedelta(days=1)),
            ]
        )
        # work item 6 is already closed, but it is outside the window so it should be skipped.
        # so we get 6 entries back in total again.

        client = Client(schema)
        query = """
                    query getProjectWorkItemsFunnelView($project_key:String!) {
                        project(key: $project_key) {
                            workItems(funnelView: true, closedWithinDays: 10) {
                                edges { 
                                    node {
                                        name
                                        displayId
                                        state
                                        stateType
                                    }
                                }
                            }
                        }
                    }
                """

        result = client.execute(query, variable_values=dict(project_key=fixture.project.key))
        assert 'data' in result
        work_items = result['data']['project']['workItems']['edges']
        assert len(work_items) == 6
        # Issue 5 shows up twice since it was closed twice.
        assert len([work_item for work_item in work_items if work_item['node']['name'] == 'Issue 5']) == 2


class TestProjectFunnelViewAggregateMetrics:

    @pytest.yield_fixture
    def setup(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name='Issue 1',
                display_id='1001',
                state='backlog',
                created_at=get_date("2018-12-02"),
                updated_at=get_date("2018-12-03"),
                **dict_merge(
                    work_items_common,
                    dict(is_bug=False, work_item_type='issue')
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
                    work_items_common,
                    dict(is_bug=False, work_item_type='issue')
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
                    work_items_common,
                    dict(is_bug=False, work_item_type='issue')
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
                    work_items_common,
                    dict(is_bug=False, work_item_type='issue')
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
                    work_items_common,
                    dict(is_bug=True, work_item_type='bug')
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
                    work_items_common,
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
                    work_items_common,
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
                    work_items_common,
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
                    work_items_common,
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
                    work_items_common,
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
                    work_items_common,
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
                    work_items_common,
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
            project=project,
            work_items=work_items,
            api_helper=api_helper,
            work_items_common=work_items_common

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
