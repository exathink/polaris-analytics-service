# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from graphene.test import Client
from datetime import timedelta
from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *


class TestProjectPipelineCycleMetricsCurrentPipeline:
    # Initially test the case for the current pipeline

    # Initially testing that it returns the right number of measurements
    # based on measurement_window  even when there are no closed
    # items in the interval
    def it_returns_the_right_number_of_measurements_1(self, api_work_items_import_fixture):
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
                    query getProjectPipelineCycleMetrics(
                        $project_key:String!, 
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [PipelineCycleMetrics], 
                            pipelineCycleMetricsArgs: {
                                metrics: [],
                            }

                        ) {
                            pipelineCycleMetrics {
                                measurementDate
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        assert project['pipelineCycleMetrics']

    def it_return_correct_results_when_there_is_one_active_and_no_closed_items(self, api_work_items_import_fixture):
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
                    query getProjectPipelineCycleMetrics(
                        $project_key:String!, 
                        $percentile: Float
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [PipelineCycleMetrics], 
                            pipelineCycleMetricsArgs: {
                                metrics: [
                                    min_lead_time,
                                    avg_lead_time,
                                    max_lead_time,
                                    percentile_lead_time,
                                    min_cycle_time,
                                    avg_cycle_time,
                                    max_cycle_time,
                                    percentile_cycle_time,
                                    work_items_in_scope,
                                    work_items_with_null_cycle_time

                                ],
                                leadTimeTargetPercentile: $percentile,
                                cycleTimeTargetPercentile: $percentile
                            }

                        ) {
                            id
                            name
                            key
                            pipelineCycleMetrics {
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
                                leadTimeTargetPercentile
                                cycleTimeTargetPercentile
                                workItemsWithNullCycleTime
                                earliestClosedDate
                                latestClosedDate
                                workItemsInScope
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert project['pipelineCycleMetrics']

        measurement = project['pipelineCycleMetrics']
        assert 0 < measurement['minLeadTime'] - 10 < 1
        assert 0 < measurement['avgLeadTime'] - 10 < 1
        assert 0 < measurement['maxLeadTime'] - 10 < 1
        assert 0 < measurement['percentileCycleTime'] - 9 < 1
        assert 0 < measurement['minCycleTime'] - 9 < 1
        assert 0 < measurement['avgCycleTime'] - 9 < 1
        assert 0 < measurement['maxCycleTime'] - 9 < 1
        assert 0 < measurement['percentileCycleTime'] - 9 < 1
        assert not measurement['earliestClosedDate']
        assert not measurement['latestClosedDate']
        assert measurement['workItemsInScope'] == 1
        assert measurement['workItemsWithNullCycleTime'] == 0

    def it_calculates_backlog_time_and_cycle_time_correctly(self, api_work_items_import_fixture):
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
        # move it into progress and accumulate some cycle time
        api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])
        # move it back to backlog to accumulate some backlog time
        api_helper.update_work_items([(0, 'backlog', start_date + timedelta(days=3))])
        # move it back to doing to restart the cycle time clock
        api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=5))])
        api_helper.update_work_items([(0, 'done', start_date + timedelta(days=6))])

        client = Client(schema)
        query = """
                    query getProjectPipelineCycleMetrics(
                        $project_key:String!, 
                        $percentile: Float
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [PipelineCycleMetrics], 
                            pipelineCycleMetricsArgs: {
                                metrics: [
                                    min_lead_time,
                                    avg_lead_time,
                                    max_lead_time,
                                    percentile_lead_time,
                                    min_cycle_time,
                                    avg_cycle_time,
                                    max_cycle_time,
                                    percentile_cycle_time,
                                    work_items_in_scope,
                                    work_items_with_null_cycle_time

                                ],
                                leadTimeTargetPercentile: $percentile,
                                cycleTimeTargetPercentile: $percentile
                            }

                        ) {
                            id
                            name
                            key
                            pipelineCycleMetrics {
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
                                leadTimeTargetPercentile
                                cycleTimeTargetPercentile
                                workItemsWithNullCycleTime
                                earliestClosedDate
                                latestClosedDate
                                workItemsInScope
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert project['pipelineCycleMetrics']

        measurement = project['pipelineCycleMetrics']
        assert 0 < measurement['minLeadTime'] - 10 < 1
        assert 0 < measurement['avgLeadTime'] - 10 < 1
        assert 0 < measurement['maxLeadTime'] - 10 < 1
        # we spent an extra 2 days in the backlog so this is the main difference
        assert 0 < measurement['percentileCycleTime'] - 7 < 1
        assert 0 < measurement['minCycleTime'] - 7 < 1
        assert 0 < measurement['avgCycleTime'] - 7 < 1
        assert 0 < measurement['maxCycleTime'] - 7 < 1
        assert 0 < measurement['percentileCycleTime'] - 7 < 1
        assert not measurement['earliestClosedDate']
        assert not measurement['latestClosedDate']
        assert measurement['workItemsInScope'] == 1
        assert measurement['workItemsWithNullCycleTime'] == 0

    def it_calculates_work_items_in_scope_and_cycle_time_correctly_for_the_initial_transition_to_an_open_state(self, api_work_items_import_fixture):

        # this test is there because we were incorrectly dropping work items in this state from the
        # the set giving wrong counts for work items in scope.

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
        # move it into progress. this is the only in progress state so we should correctly report
        # the cycle time as 9
        api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])


        client = Client(schema)
        query = """
                    query getProjectPipelineCycleMetrics(
                        $project_key:String!, 
                        $percentile: Float
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [PipelineCycleMetrics], 
                            pipelineCycleMetricsArgs: {
                                metrics: [
                                    min_lead_time,
                                    avg_lead_time,
                                    max_lead_time,
                                    percentile_lead_time,
                                    min_cycle_time,
                                    avg_cycle_time,
                                    max_cycle_time,
                                    percentile_cycle_time,
                                    work_items_in_scope,
                                    work_items_with_null_cycle_time

                                ],
                                leadTimeTargetPercentile: $percentile,
                                cycleTimeTargetPercentile: $percentile
                            }

                        ) {
                            id
                            name
                            key
                            pipelineCycleMetrics {
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
                                leadTimeTargetPercentile
                                cycleTimeTargetPercentile
                                workItemsWithNullCycleTime
                                earliestClosedDate
                                latestClosedDate
                                workItemsInScope
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert project['pipelineCycleMetrics']

        measurement = project['pipelineCycleMetrics']
        assert 0 < measurement['minLeadTime'] - 10 < 1
        assert 0 < measurement['avgLeadTime'] - 10 < 1
        assert 0 < measurement['maxLeadTime'] - 10 < 1
        assert 0 < measurement['percentileCycleTime'] - 9 < 1
        assert 0 < measurement['minCycleTime'] - 9 < 1
        assert 0 < measurement['avgCycleTime'] - 9 < 1
        assert 0 < measurement['maxCycleTime'] - 9 < 1
        assert 0 < measurement['percentileCycleTime'] - 9 < 1
        assert not measurement['earliestClosedDate']
        assert not measurement['latestClosedDate']
        assert measurement['workItemsInScope'] == 1
        assert measurement['workItemsWithNullCycleTime'] == 0

    def it_return_correct_results_when_there_are_no_active_and_one_closed_item(self, api_work_items_import_fixture):
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
                    query getProjectPipelineCycleMetrics(
                        $project_key:String!, 
                        $percentile: Float
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [PipelineCycleMetrics], 
                            pipelineCycleMetricsArgs: {
                                metrics: [
                                    min_lead_time,
                                    avg_lead_time,
                                    max_lead_time,
                                    percentile_lead_time,
                                    min_cycle_time,
                                    avg_cycle_time,
                                    max_cycle_time,
                                    percentile_cycle_time,
                                    work_items_in_scope,
                                    work_items_with_null_cycle_time

                                ],
                                leadTimeTargetPercentile: $percentile,
                                cycleTimeTargetPercentile: $percentile
                            }

                        ) {
                            pipelineCycleMetrics {
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
                                leadTimeTargetPercentile
                                cycleTimeTargetPercentile
                                workItemsWithNullCycleTime
                                earliestClosedDate
                                latestClosedDate
                                workItemsInScope
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert project['pipelineCycleMetrics']
        # In this case we have 1 closed and 2 backlog items. all should be
        # filtered out and we should get null results for all cycle metrics.
        measurement = project['pipelineCycleMetrics']
        assert not measurement['minLeadTime']
        assert not measurement['avgLeadTime']
        assert not measurement['maxLeadTime']
        assert not measurement['percentileLeadTime']
        assert not measurement['minCycleTime']
        assert not measurement['avgCycleTime']
        assert not measurement['maxCycleTime']
        assert not measurement['percentileCycleTime']
        assert not measurement['earliestClosedDate']
        assert not measurement['latestClosedDate']
        assert measurement['workItemsInScope'] == 0
        assert measurement['workItemsWithNullCycleTime'] == 0

    def it_return_correct_results_when_there_are_active_items_on_the_end_date_of_the_measurement_period(self,
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

        api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])
        api_helper.update_work_items([(0, 'done', start_date + timedelta(days=10))])

        client = Client(schema)
        query = """
                    query getProjectPipelineCycleMetrics(
                        $project_key:String!, 
                        $percentile: Float
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [PipelineCycleMetrics], 
                            pipelineCycleMetricsArgs: {
                                metrics: [
                                    min_lead_time,
                                    avg_lead_time,
                                    max_lead_time,
                                    percentile_lead_time,
                                    min_cycle_time,
                                    avg_cycle_time,
                                    max_cycle_time,
                                    percentile_cycle_time,
                                    work_items_in_scope,
                                    work_items_with_null_cycle_time

                                ],
                                leadTimeTargetPercentile: $percentile,
                                cycleTimeTargetPercentile: $percentile
                            }

                        ) {
                            pipelineCycleMetrics {
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
                                leadTimeTargetPercentile
                                cycleTimeTargetPercentile
                                workItemsWithNullCycleTime
                                earliestClosedDate
                                latestClosedDate
                                workItemsInScope
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert project['pipelineCycleMetrics']
        # there is one work item that closed on the  end of the measurement period
        # so the last entry will record the metrics for this work item, the rest will
        # be empty
        measurement = project['pipelineCycleMetrics']
        assert 0 < measurement['minLeadTime'] - 10.0 < 1
        assert 0 < measurement['avgLeadTime'] - 10.0 < 1
        assert 0 < measurement['maxLeadTime'] - 10.0 < 1
        assert 0 < measurement['percentileLeadTime'] - 10.0 < 1
        assert 0 < measurement['minCycleTime'] - 9.0 < 1
        assert 0 < measurement['avgCycleTime'] - 9.0 < 1
        assert 0 < measurement['maxCycleTime'] - 9.0 < 1
        assert 0 < measurement['percentileCycleTime'] - 9.0 < 1
        assert  not measurement['earliestClosedDate']
        assert not measurement['latestClosedDate']
        assert measurement['workItemsInScope'] == 1
        assert measurement['workItemsWithNullCycleTime'] == 0

    def it_returns_correct_results_when_there_are_multiple_active_items(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=30)

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
        # active T+10
        api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=10))])

        #  T+20
        api_helper.update_work_items([(1, 'upnext', start_date + timedelta(days=20))])

        # closed T+25
        api_helper.update_work_items([(2, 'upnext', start_date + timedelta(days=25))])

        client = Client(schema)
        query = """
                    query getProjectPipelineCycleMetrics(
                        $project_key:String!, 
                        $percentile: Float
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [PipelineCycleMetrics], 
                            pipelineCycleMetricsArgs: {
                                metrics: [
                                    min_lead_time,
                                    avg_lead_time,
                                    max_lead_time,
                                    percentile_lead_time,
                                    min_cycle_time,
                                    avg_cycle_time,
                                    max_cycle_time,
                                    percentile_cycle_time,
                                    work_items_in_scope,
                                    work_items_with_null_cycle_time

                                ],
                                leadTimeTargetPercentile: $percentile,
                                cycleTimeTargetPercentile: $percentile
                            }

                        ) {
                            pipelineCycleMetrics {
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
                                leadTimeTargetPercentile
                                cycleTimeTargetPercentile
                                workItemsWithNullCycleTime
                                earliestClosedDate
                                latestClosedDate
                                workItemsInScope
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert project['pipelineCycleMetrics']
        measurement = project['pipelineCycleMetrics']
        assert 0 < measurement['minLeadTime'] - 30.0 < 1
        assert 0 < measurement['avgLeadTime'] - 30.0 < 1
        assert 0 < measurement['maxLeadTime'] - 30.0 < 1
        assert 0 < measurement['percentileLeadTime'] - 30.0 < 1
        assert 0 < measurement['minCycleTime'] - 5.0 < 1
        assert 0 < measurement['avgCycleTime'] - 11.0 < 1
        assert 0 < measurement['maxCycleTime'] - 20.0 < 1
        assert 0 < measurement['percentileCycleTime'] - 20.0 < 1
        assert not measurement['earliestClosedDate']
        assert not measurement['latestClosedDate']
        assert measurement['workItemsInScope'] == 3
        assert measurement['workItemsWithNullCycleTime'] == 0

    def it_reports_work_items_with_commits_correctly(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id=f'1000{i}',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )
            for i in range(0, 4)
        ]

        api_helper.import_work_items(work_items)

        api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=6))])
        # update commit count for item 0
        api_helper.update_delivery_cycles(([(0, dict(property='commit_count', value=3))]))
        # the next work item is active and has commits too andshould be reported in the commit count trends.
        api_helper.update_work_items([(1, 'done', start_date + timedelta(days=1))])
        api_helper.update_delivery_cycles(([(1, dict(property='commit_count', value=1))]))
        # work item 3 on the other hand has commits but is closed, so it should not be reported in the result
        api_helper.update_work_items([(2, 'closed', start_date + timedelta(days=5))])
        api_helper.update_delivery_cycles(([(2, dict(property='commit_count', value=2))]))

        # work item 4 has no commits but is active so it should be reported as a work item in scope but
        # not as one with commits.
        api_helper.update_work_items([(3, 'upnext', start_date + timedelta(days=6))])

        client = Client(schema)
        query = """
                    query getProjectPipelineCycleMetrics(
                        $project_key:String!, 
                        $percentile: Float
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [PipelineCycleMetrics], 
                            pipelineCycleMetricsArgs: {
                                metrics: [
                                    max_lead_time,
                                    max_cycle_time,
                                    work_items_in_scope,
                                    work_items_with_commits
                                ],
                                leadTimeTargetPercentile: $percentile,
                                cycleTimeTargetPercentile: $percentile
                            }

                        ) {
                            pipelineCycleMetrics {
                                measurementDate
                                measurementWindow
                                maxLeadTime
                                maxCycleTime
                                workItemsWithCommits
                                workItemsInScope
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert project['pipelineCycleMetrics']
        # there is one work item that closed 6 days before the end of the measurement period
        # so the last 6 dates will record the metrics for this work item, the rest will
        # be empty
        measurement = project['pipelineCycleMetrics']
        assert 0 < measurement['maxLeadTime'] - 10.0 < 1
        assert 0 < measurement['maxCycleTime'] - 9.0 < 1
        assert 0 < measurement['workItemsInScope'] == 3
        assert 0 < measurement['workItemsWithCommits'] == 2

    def it_reports_quartiles_correctly(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id=f'1000{i}',
                state='backlog',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )
            for i in range(0, 5)
        ]

        api_helper.import_work_items(work_items)
        for i in range(0, 5):
            # move them out of backlog them so that cycle times are distributed in the sequence (1,2,3,4,5)
            api_helper.update_work_items([(i, 'upnext', start_date + timedelta(days=9 - i))])

        client = Client(schema)
        query = """
                    query getProjectPipelineCycleMetrics(
                        $project_key:String!, 
                        $percentile: Float
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [PipelineCycleMetrics], 
                            pipelineCycleMetricsArgs: {
                                metrics: [
                                    min_cycle_time, 
                                    q1_cycle_time, 
                                    median_cycle_time, 
                                    q3_cycle_time, 
                                    max_cycle_time
                                ],
                                leadTimeTargetPercentile: $percentile,
                                cycleTimeTargetPercentile: $percentile
                            }

                        ) {
                            pipelineCycleMetrics {
                                measurementDate
                                measurementWindow
                                minCycleTime
                                q1CycleTime
                                medianCycleTime
                                q3CycleTime
                                maxCycleTime
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert project['pipelineCycleMetrics']
        measurement = project['pipelineCycleMetrics']
        assert 0 < measurement['minCycleTime'] - 1.0 < 1
        assert 0 < measurement['q1CycleTime'] - 2.0 < 1
        assert 0 < measurement['medianCycleTime'] - 3.0 < 1
        assert 0 < measurement['q3CycleTime'] - 4.0 < 1
        assert 0 < measurement['maxCycleTime'] - 5.0 < 1

    def it_filters_epics_and_includes_sub_tasks_by_default(self, api_work_items_import_fixture):
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

        work_items[0]['is_epic'] = True
        work_items[1]['work_item_type'] = 'subtask'

        api_helper.import_work_items(work_items)

        for i in range(0, 3):
            api_helper.update_work_items([(i, 'upnext', start_date + timedelta(days=1))])


        client = Client(schema)
        query = """
                query getProjectPipelineCycleMetrics(
                    $project_key:String!, 
                    $percentile: Float
                ) {
                    project(
                        key: $project_key, 
                        interfaces: [PipelineCycleMetrics], 
                        pipelineCycleMetricsArgs: {
                            metrics: [
                                work_items_in_scope
                            ],
                            leadTimeTargetPercentile: $percentile,
                            cycleTimeTargetPercentile: $percentile
                        }

                    ) {
                        pipelineCycleMetrics {
                            workItemsInScope
                        }
                    }
                }
            """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert project['pipelineCycleMetrics']
        measurement = project['pipelineCycleMetrics']
        assert measurement['workItemsInScope'] == 2



    def it_includes_epics_and_filters_sub_tasks_when_specified(self, api_work_items_import_fixture):
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

        work_items[0]['work_item_type'] = 'epic'
        work_items[1]['work_item_type'] = 'subtask'

        api_helper.import_work_items(work_items)

        for i in range(0, 3):
            api_helper.update_work_items([(i, 'upnext', start_date + timedelta(days=1))])


        client = Client(schema)
        query = """
                query getProjectPipelineCycleMetrics(
                    $project_key:String!, 
                    $percentile: Float
                ) {
                    project(
                        key: $project_key, 
                        interfaces: [PipelineCycleMetrics], 
                        pipelineCycleMetricsArgs: {
                            metrics: [
                                work_items_in_scope
                            ],
                            leadTimeTargetPercentile: $percentile,
                            cycleTimeTargetPercentile: $percentile, 
                            includeEpics: true,
                            includeSubTasks: false
                        }

                    ) {
                        pipelineCycleMetrics {
                            workItemsInScope
                        }
                    }
                }
            """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert project['pipelineCycleMetrics']
        measurement = project['pipelineCycleMetrics']
        assert measurement['workItemsInScope'] == 2


    def it_limits_analysis_to_defects_only_when_specified(self, api_work_items_import_fixture):
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

        # the default fixture sets everything to is_bug=True so we flip to set up this test.
        work_items[0]['is_bug'] = False
        work_items[1]['is_bug'] = False

        api_helper.import_work_items(work_items)

        for i in range(0, 3):
            api_helper.update_work_items([(i, 'upnext', start_date + timedelta(days=1))])


        client = Client(schema)
        query = """
                query getProjectPipelineCycleMetrics(
                    $project_key:String!, 
                    $percentile: Float
                ) {
                    project(
                        key: $project_key, 
                        interfaces: [PipelineCycleMetrics], 
                        pipelineCycleMetricsArgs: {
                            metrics: [
                                work_items_in_scope
                            ],
                            leadTimeTargetPercentile: $percentile,
                            cycleTimeTargetPercentile: $percentile, 
                            defectsOnly: true
                        }

                    ) {
                        pipelineCycleMetrics {
                            workItemsInScope
                        }
                    }
                }
            """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert project['pipelineCycleMetrics']
        measurement = project['pipelineCycleMetrics']
        assert measurement['workItemsInScope'] == 1


    def it_limits_analysis_to_specs_only_when_specified(self, api_work_items_import_fixture):
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
        api_helper.update_delivery_cycles([(0, dict(property='commit_count', value=2))])

        for i in range(0, 3):
            api_helper.update_work_items([(i, 'upnext', start_date + timedelta(days=2))])


        client = Client(schema)
        query = """
                query getProjectPipelineCycleMetrics(
                    $project_key:String!, 
                    $percentile: Float
                ) {
                    project(
                        key: $project_key, 
                        interfaces: [PipelineCycleMetrics], 
                        pipelineCycleMetricsArgs: {
                            metrics: [
                                work_items_in_scope,
                                avg_cycle_time
                            ],
                            leadTimeTargetPercentile: $percentile,
                            cycleTimeTargetPercentile: $percentile, 
                            specsOnly: true
                        }

                    ) {
                        pipelineCycleMetrics {
                            workItemsInScope
                            avgCycleTime
                        }
                    }
                }
            """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert project['pipelineCycleMetrics']
        measurement = project['pipelineCycleMetrics']
        assert measurement['workItemsInScope'] == 1


    def it_reports_latency_metrics_correctly(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        start_date = datetime.utcnow() - timedelta(days=10)

        work_items = [
            dict(
                key=uuid.uuid4().hex,
                name=f'Issue {i}',
                display_id=f'1000{i}',
                state='doing',
                created_at=start_date,
                updated_at=start_date,
                **work_items_common
            )
            for i in range(0, 5)
        ]

        api_helper.import_work_items(work_items)
        for i in range(0, 5):
            # expect latencies to be distributed as 1, 2, 3, 4, 5
            api_helper.update_delivery_cycles(
                ([(i, dict(property='latest_commit', value=start_date + timedelta(days=5+i)))]))



        client = Client(schema)
        query = """
                    query getProjectPipelineCycleMetricsTrends(
                        $project_key:String!, 
                        $percentile: Float
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [PipelineCycleMetrics], 
                            pipelineCycleMetricsArgs: {
                        
                                metrics: [
                                   min_latency
                                   max_latency
                                   avg_latency
                                   percentile_latency 
                                ],
                                latencyTargetPercentile: $percentile,
                            }

                        ) {
                            pipelineCycleMetrics {
                                measurementDate
                
                                minLatency
                                maxLatency
                                avgLatency
                                percentileLatency
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        measurement = project['pipelineCycleMetrics']
        assert 0 < measurement['minLatency'] - 1.0 < 0.01
        assert 0 < measurement['avgLatency'] - 3.0 < 0.01
        assert 0 < measurement['maxLatency'] - 5.0 < 0.01
        assert 0 < measurement['percentileLatency'] - 4.0 < 0.01
