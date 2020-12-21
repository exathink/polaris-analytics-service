from graphene.test import Client
from datetime import timedelta
from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *


class TestProjectCycleMetricsTrends:

    # Initially testing that it returns the right number of measurements
    # based on measurement_window and sampling_frequency even when there are no closed
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
                    query getProjectCycleMetricsTrends(
                        $project_key:String!, 
                        $days: Int!, 
                        $window: Int!,
                        $sample: Int
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [CycleMetricsTrends], 
                            cycleMetricsTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
                                metrics: [],
                            }

                        ) {
                            cycleMetricsTrends {
                                measurementDate
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            days=30,
            window=7,
            sample=1,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert len(project['cycleMetricsTrends']) == 31

    def it_returns_the_right_number_of_measurements_2(self, api_work_items_import_fixture):
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
                    query getProjectCycleMetricsTrends(
                        $project_key:String!, 
                        $days: Int!, 
                        $window: Int!,
                        $sample: Int
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [CycleMetricsTrends], 
                            cycleMetricsTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
                                metrics: [],
                            }

                        ) {
                            cycleMetricsTrends {
                                measurementDate
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            days=30,
            # changing window - should have no impact on result
            window=30,
            sample=1,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert len(project['cycleMetricsTrends']) == 31

    def it_returns_the_right_number_of_measurements_3(self, api_work_items_import_fixture):
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
                    query getProjectCycleMetricsTrends(
                        $project_key:String!, 
                        $days: Int!, 
                        $window: Int!,
                        $sample: Int
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [CycleMetricsTrends], 
                            cycleMetricsTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
                                metrics: [],
                            }

                        ) {
                            cycleMetricsTrends {
                                measurementDate
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            days=30,
            window=7,
            # changing sample - should reduce measurement
            sample=7,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        assert len(project['cycleMetricsTrends']) == 5

    def it_return_correct_results_when_there_are_no_closed_items(self, api_work_items_import_fixture):
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
                    query getProjectCycleMetricsTrends(
                        $project_key:String!, 
                        $days: Int!, 
                        $window: Int!,
                        $sample: Int,
                        $percentile: Float
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [CycleMetricsTrends], 
                            cycleMetricsTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
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
                                    work_items_with_null_cycle_time,
                                    cadence

                                ],
                                leadTimeTargetPercentile: $percentile,
                                cycleTimeTargetPercentile: $percentile
                            }

                        ) {
                            id
                            name
                            key
                            cycleMetricsTrends {
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
                                cadence
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            days=30,
            window=7,
            sample=1,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert len(project['cycleMetricsTrends']) == 31
        # since there are no work items in the whole range, the metrics for each point will be None.
        for measurement in project['cycleMetricsTrends']:
            assert not measurement['minLeadTime']
            assert not measurement['avgLeadTime']
            assert not measurement['maxLeadTime']
            assert not measurement['percentileCycleTime']
            assert not measurement['minCycleTime']
            assert not measurement['avgCycleTime']
            assert not measurement['maxCycleTime']
            assert not measurement['percentileCycleTime']
            assert not measurement['earliestClosedDate']
            assert not measurement['latestClosedDate']
            assert measurement['workItemsInScope'] == 0
            assert measurement['workItemsWithNullCycleTime'] == 0
            assert measurement['cadence'] == 0

    def it_return_correct_results_when_there_is_one_closed_item(self, api_work_items_import_fixture):
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
                    query getProjectCycleMetricsTrends(
                        $project_key:String!, 
                        $days: Int!, 
                        $window: Int!,
                        $sample: Int,
                        $percentile: Float
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [CycleMetricsTrends], 
                            cycleMetricsTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
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
                                    work_items_with_null_cycle_time,
                                    cadence

                                ],
                                leadTimeTargetPercentile: $percentile,
                                cycleTimeTargetPercentile: $percentile
                            }

                        ) {
                            cycleMetricsTrends {
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
                                cadence
                                earliestClosedDate
                                latestClosedDate
                                workItemsInScope
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            days=30,
            window=7,
            sample=1,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert len(project['cycleMetricsTrends']) == 31
        # there is one work item that closed 6 days before the end of the measurement period
        # so the last 5 dates will record the metrics for this work item, the rest will
        # be empty
        for index, measurement in enumerate(project['cycleMetricsTrends']):
            if index < 5:
                assert measurement['minLeadTime'] == 6.0
                assert measurement['avgLeadTime'] == 6.0
                assert measurement['maxLeadTime'] == 6.0
                assert measurement['percentileLeadTime']
                assert measurement['minCycleTime'] == 5.0
                assert measurement['avgCycleTime'] == 5.0
                assert measurement['maxCycleTime'] == 5.0
                assert measurement['percentileCycleTime'] == 5.0
                assert measurement['earliestClosedDate']
                assert measurement['latestClosedDate']
                assert measurement['workItemsInScope'] == 1
                assert measurement['workItemsWithNullCycleTime'] == 0
                assert measurement['cadence'] == 1
            else:
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
                assert measurement['cadence'] == 0

    def it_return_correct_results_when_there_are_closed_item_on_the_end_date_of_the_measurement_period(self,
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
        api_helper.update_work_items([(0, 'done', start_date + timedelta(days=4))])
        api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=10))])

        client = Client(schema)
        query = """
                    query getProjectCycleMetricsTrends(
                        $project_key:String!, 
                        $days: Int!, 
                        $window: Int!,
                        $sample: Int,
                        $percentile: Float
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [CycleMetricsTrends], 
                            cycleMetricsTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
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
                            cycleMetricsTrends {
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
            days=30,
            window=7,
            sample=1,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert len(project['cycleMetricsTrends']) == 31
        # there is one work item that closed on the  end of the measurement period
        # so the last entry will record the metrics for this work item, the rest will
        # be empty
        for index, measurement in enumerate(project['cycleMetricsTrends']):
            if index < 1:
                assert measurement['minLeadTime'] == 10.0
                assert measurement['avgLeadTime'] == 10.0
                assert measurement['maxLeadTime'] == 10.0
                assert measurement['percentileLeadTime']
                assert measurement['minCycleTime'] == 9.0
                assert measurement['avgCycleTime'] == 9.0
                assert measurement['maxCycleTime'] == 9.0
                assert measurement['percentileCycleTime'] == 9.0
                assert measurement['earliestClosedDate']
                assert measurement['latestClosedDate']
                assert measurement['workItemsInScope'] == 1
                assert measurement['workItemsWithNullCycleTime'] == 0
            else:
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

    def it_returns_correct_results_when_window_covers_different_numbers_of_points(self, api_work_items_import_fixture):
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
        # closed T+10
        api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=10))])

        # closed T+20
        api_helper.update_work_items([(1, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=20))])

        # closed T+25
        api_helper.update_work_items([(2, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(2, 'closed', start_date + timedelta(days=25))])

        client = Client(schema)
        query = """
                    query getProjectCycleMetricsTrends(
                        $project_key:String!, 
                        $days: Int!, 
                        $window: Int!,
                        $sample: Int,
                        $percentile: Float
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [CycleMetricsTrends], 
                            cycleMetricsTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
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
                            cycleMetricsTrends {
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
            days=30,
            window=7,
            sample=1,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert len(project['cycleMetricsTrends']) == 31

        for index, measurement in enumerate(project['cycleMetricsTrends']):
            if index < 4:
                assert measurement['minLeadTime'] == 25.0
                assert measurement['avgLeadTime'] == 25.0
                assert measurement['maxLeadTime'] == 25.0
                assert measurement['percentileLeadTime']
                assert measurement['minCycleTime'] == 24.0
                assert measurement['avgCycleTime'] == 24.0
                assert measurement['maxCycleTime'] == 24.0
                assert measurement['percentileCycleTime'] == 24.0
                assert measurement['earliestClosedDate']
                assert measurement['latestClosedDate']
                assert measurement['workItemsInScope'] == 1
                assert measurement['workItemsWithNullCycleTime'] == 0
            elif index < 6:
                assert measurement['minLeadTime'] == 20.0
                assert measurement['avgLeadTime'] == 22.5
                assert measurement['maxLeadTime'] == 25.0
                assert measurement['percentileLeadTime']
                assert measurement['minCycleTime'] == 19.0
                assert measurement['avgCycleTime'] == 21.5
                assert measurement['maxCycleTime'] == 24.0
                assert measurement['percentileCycleTime'] == 24.0
                assert measurement['earliestClosedDate']
                assert measurement['latestClosedDate']
                assert measurement['workItemsInScope'] == 2
                assert measurement['workItemsWithNullCycleTime'] == 0
            elif index < 11:
                assert measurement['minLeadTime'] == 20.0
                assert measurement['avgLeadTime'] == 20.0
                assert measurement['maxLeadTime'] == 20.0
                assert measurement['percentileLeadTime']
                assert measurement['minCycleTime'] == 19.0
                assert measurement['avgCycleTime'] == 19.0
                assert measurement['maxCycleTime'] == 19.0
                assert measurement['percentileCycleTime'] == 19.0
                assert measurement['earliestClosedDate']
                assert measurement['latestClosedDate']
                assert measurement['workItemsInScope'] == 1
                assert measurement['workItemsWithNullCycleTime'] == 0
            elif index < 14:
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
            elif index < 21:
                assert measurement['minLeadTime'] == 10.0
                assert measurement['avgLeadTime'] == 10.0
                assert measurement['maxLeadTime'] == 10.0
                assert measurement['percentileLeadTime']
                assert measurement['minCycleTime'] == 9.0
                assert measurement['avgCycleTime'] == 9.0
                assert measurement['maxCycleTime'] == 9.0
                assert measurement['percentileCycleTime'] == 9.0
                assert measurement['earliestClosedDate']
                assert measurement['latestClosedDate']
                assert measurement['workItemsInScope'] == 1
                assert measurement['workItemsWithNullCycleTime'] == 0
            else:
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

    # same test as last one but with a different window size
    def it_returns_correct_results_for_multiple_points_for_a_different_window_size(self, api_work_items_import_fixture):
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
        # closed T+10
        api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=10))])

        # closed T+20
        api_helper.update_work_items([(1, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=20))])

        # closed T+25
        api_helper.update_work_items([(2, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(2, 'closed', start_date + timedelta(days=25))])

        client = Client(schema)
        query = """
                    query getProjectCycleMetricsTrends(
                        $project_key:String!, 
                        $days: Int!, 
                        $window: Int!,
                        $sample: Int,
                        $percentile: Float
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [CycleMetricsTrends], 
                            cycleMetricsTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
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
                            cycleMetricsTrends {
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
            days=30,
            window=15,
            sample=1,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert len(project['cycleMetricsTrends']) == 31

        for index, measurement in enumerate(project['cycleMetricsTrends']):
            if index < 6:
                assert measurement['minLeadTime'] == 20.0
                assert measurement['avgLeadTime'] == 22.5
                assert measurement['maxLeadTime'] == 25.0
                assert measurement['percentileLeadTime']
                assert measurement['minCycleTime'] == 19.0
                assert measurement['avgCycleTime'] == 21.5
                assert measurement['maxCycleTime'] == 24.0
                assert measurement['percentileCycleTime'] == 24.0
                assert measurement['earliestClosedDate']
                assert measurement['latestClosedDate']
                assert measurement['workItemsInScope'] == 2
                assert measurement['workItemsWithNullCycleTime'] == 0

            elif index < 11:
                assert measurement['minLeadTime'] == 10.0
                assert measurement['avgLeadTime'] == 15.0
                assert measurement['maxLeadTime'] == 20.0
                assert measurement['percentileLeadTime']
                assert measurement['minCycleTime'] == 9.0
                assert measurement['avgCycleTime'] == 14.0
                assert measurement['maxCycleTime'] == 19.0
                assert measurement['percentileCycleTime'] == 19.0
                assert measurement['earliestClosedDate']
                assert measurement['latestClosedDate']
                assert measurement['workItemsInScope'] == 2
                assert measurement['workItemsWithNullCycleTime'] == 0
            elif index < 21:
                assert measurement['minLeadTime'] == 10.0
                assert measurement['avgLeadTime'] == 10.0
                assert measurement['maxLeadTime'] == 10.0
                assert measurement['percentileLeadTime']
                assert measurement['minCycleTime'] == 9.0
                assert measurement['avgCycleTime'] == 9.0
                assert measurement['maxCycleTime'] == 9.0
                assert measurement['percentileCycleTime'] == 9.0
                assert measurement['earliestClosedDate']
                assert measurement['latestClosedDate']
                assert measurement['workItemsInScope'] == 1
                assert measurement['workItemsWithNullCycleTime'] == 0
            else:
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

    def it_reports_work_items_with_null_cycle_times_correctly(self, api_work_items_import_fixture):
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
        # move from open to closed directly so there is no cycle time recorded.
        api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=6))])

        client = Client(schema)
        query = """
                    query getProjectCycleMetricsTrends(
                        $project_key:String!, 
                        $days: Int!, 
                        $window: Int!,
                        $sample: Int,
                        $percentile: Float
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [CycleMetricsTrends], 
                            cycleMetricsTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
                                metrics: [
                                    max_lead_time,
                                    max_cycle_time,
                                    work_items_in_scope,
                                    work_items_with_null_cycle_time
                                ],
                                leadTimeTargetPercentile: $percentile,
                                cycleTimeTargetPercentile: $percentile
                            }

                        ) {
                            cycleMetricsTrends {
                                measurementDate
                                measurementWindow
                                maxLeadTime
                                maxCycleTime
                                workItemsWithNullCycleTime
                                workItemsInScope
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            days=30,
            window=7,
            sample=7,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert len(project['cycleMetricsTrends']) == 5
        # there is one work item that closed 6 days before the end of the measurement period
        # so the last 6 dates will record the metrics for this work item, the rest will
        # be empty
        for index, measurement in enumerate(project['cycleMetricsTrends']):
            if index > 0:
                assert not measurement['maxLeadTime']
                assert not measurement['maxCycleTime']
                assert measurement['workItemsWithNullCycleTime'] == 0
            else:
                # last period should record one work item with null cycle time.
                assert measurement['maxLeadTime'] == 6.0
                assert not measurement['maxCycleTime']
                assert measurement['workItemsInScope'] == 1
                assert measurement['workItemsWithNullCycleTime'] == 1

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
        # move from open to closed directly so there is no cycle time recorded.
        api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=6))])
        # update commit count for item 0
        api_helper.update_delivery_cycles(([(0, dict(property='commit_count', value=3))]))
        # the next work item has a valid cycle time, but either should be reported in the cycle metrics trends.
        api_helper.update_work_items([(1, 'upnext', start_date + timedelta(days=1))])
        api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=5))])
        # add some commits to this one too.
        api_helper.update_delivery_cycles(([(1, dict(property='commit_count', value=1))]))
        # work item 3 on the other hand has commits but is not closed, so it should not be reported in the result
        api_helper.update_delivery_cycles(([(2, dict(property='commit_count', value=2))]))

        # work item 4 has not commits and is closed so it should not be reported as a work item in scope but
        # not as one which has commit counts > 0
        api_helper.update_work_items([(3, 'closed', start_date + timedelta(days=6))])

        client = Client(schema)
        query = """
                    query getProjectCycleMetricsTrends(
                        $project_key:String!, 
                        $days: Int!, 
                        $window: Int!,
                        $sample: Int,
                        $percentile: Float
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [CycleMetricsTrends], 
                            cycleMetricsTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
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
                            cycleMetricsTrends {
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
            days=30,
            window=7,
            sample=7,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert len(project['cycleMetricsTrends']) == 5
        # there is one work item that closed 6 days before the end of the measurement period
        # so the last 6 dates will record the metrics for this work item, the rest will
        # be empty
        for index, measurement in enumerate(project['cycleMetricsTrends']):
            if index > 0:
                assert not measurement['maxLeadTime']
                assert not measurement['maxCycleTime']
                assert measurement['workItemsInScope'] == 0
                assert measurement['workItemsWithCommits'] == 0
            else:

                assert measurement['maxLeadTime'] == 6.0
                assert measurement['maxCycleTime'] == 4.0
                assert measurement['workItemsInScope'] == 3
                assert measurement['workItemsWithCommits'] == 2

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
            # move it into open phase so it has a cycle time
            api_helper.update_work_items([(i, 'upnext', start_date + timedelta(days=1))])
            # close them so that cycle times are distributed in the sequence (1,2,3,4,5)
            api_helper.update_work_items([(i, 'closed', start_date + timedelta(days=i + 2))])

        client = Client(schema)
        query = """
                    query getProjectCycleMetricsTrends(
                        $project_key:String!, 
                        $days: Int!, 
                        $window: Int!,
                        $sample: Int,
                        $percentile: Float
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [CycleMetricsTrends], 
                            cycleMetricsTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
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
                            cycleMetricsTrends {
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
            days=30,
            window=10,
            sample=10,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert len(project['cycleMetricsTrends']) == 4
        # there is one work item that closed 6 days before the end of the measurement period
        # so the last 6 dates will record the metrics for this work item, the rest will
        # be empty
        for index, measurement in enumerate(project['cycleMetricsTrends']):
            if index == 0:
                assert measurement['minCycleTime'] == 1.0
                assert measurement['q1CycleTime'] == 2.0
                assert measurement['medianCycleTime'] == 3.0
                assert measurement['q3CycleTime'] == 4.0
                assert measurement['maxCycleTime'] == 5.0
            else:
                assert not measurement['minCycleTime']
                assert not measurement['q1CycleTime']
                assert not measurement['medianCycleTime']
                assert not measurement['q3CycleTime']
                assert not measurement['maxCycleTime']

    def it_reports_effort_metrics_correctly(self, api_work_items_import_fixture):
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
            # close it so that shows up in the metrics
            api_helper.update_work_items([(i, 'closed', start_date + timedelta(days=1))])
            # close them so that cycle times and efforts are distributed in the sequence (1,2,3,4,5)
            api_helper.update_delivery_cycles(([(i, dict(property='effort', value=i + 1))]))

        client = Client(schema)
        query = """
                    query getProjectCycleMetricsTrends(
                        $project_key:String!, 
                        $days: Int!, 
                        $window: Int!,
                        $sample: Int,
                        $percentile: Float
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [CycleMetricsTrends], 
                            cycleMetricsTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
                                metrics: [
                                    total_effort
                                    avg_effort
                                    min_effort
                                    max_effort
                                    
                                ],
                                leadTimeTargetPercentile: $percentile,
                                cycleTimeTargetPercentile: $percentile
                            }

                        ) {
                            cycleMetricsTrends {
                                measurementDate
                                measurementWindow
                                minEffort
                                maxEffort
                                avgEffort
                                totalEffort
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            days=30,
            window=10,
            sample=10,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert len(project['cycleMetricsTrends']) == 4
        for index, measurement in enumerate(project['cycleMetricsTrends']):
            if index == 0:
                assert measurement['minEffort'] == 1.0
                assert measurement['avgEffort'] == 3.0
                assert measurement['maxEffort'] == 5.0
                assert measurement['totalEffort'] == 15.0
            else:
                assert not measurement['minEffort']
                assert not measurement['avgEffort']
                assert not measurement['maxEffort']
                assert not measurement['totalEffort']

    def it_reports_duration_metrics_correctly(self, api_work_items_import_fixture):
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
            # close it so that shows up in the metrics
            api_helper.update_work_items([(i, 'closed', start_date + timedelta(days=1))])
            # close them so that durations are distributed in the sequence (1,2,3,4,5)
            api_helper.update_delivery_cycles(([(i, dict(property='earliest_commit', value=start_date))]))
            api_helper.update_delivery_cycles(
                ([(i, dict(property='latest_commit', value=start_date + timedelta(days=i + 1)))]))

        client = Client(schema)
        query = """
                    query getProjectCycleMetricsTrends(
                        $project_key:String!, 
                        $days: Int!, 
                        $window: Int!,
                        $sample: Int,
                        $percentile: Float
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [CycleMetricsTrends], 
                            cycleMetricsTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
                                metrics: [
                                    min_duration
                                    avg_duration
                                    max_duration
                                    percentile_duration

                                ],
                                durationTargetPercentile: $percentile,
                            }

                        ) {
                            cycleMetricsTrends {
                                measurementDate
                                measurementWindow
                                minDuration
                                maxDuration
                                avgDuration
                                percentileDuration
                            }
                        }
                    }
                """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            days=30,
            window=10,
            sample=10,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert len(project['cycleMetricsTrends']) == 4
        for index, measurement in enumerate(project['cycleMetricsTrends']):
            if index == 0:
                assert measurement['minDuration'] == 1.0
                assert measurement['avgDuration'] == 3.0
                assert measurement['maxDuration'] == 5.0
                assert measurement['percentileDuration'] == 4.0

            else:
                assert not measurement['minDuration']
                assert not measurement['avgDuration']
                assert not measurement['maxDuration']
                assert not measurement['percentileDuration']

    def it_reports_latency_metrics_correctly(self, api_work_items_import_fixture):
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
            # expect latencies to be distributed as 1, 2, 3, 4, 5
            api_helper.update_delivery_cycles(
                ([(i, dict(property='latest_commit', value=start_date + timedelta(days=i)))]))
            # close it so that shows up in the metrics
            api_helper.update_work_items([(i, 'closed', start_date + timedelta(days=5))])


        client = Client(schema)
        query = """
                    query getProjectCycleMetricsTrends(
                        $project_key:String!, 
                        $days: Int!, 
                        $window: Int!,
                        $sample: Int,
                        $percentile: Float
                    ) {
                        project(
                            key: $project_key, 
                            interfaces: [CycleMetricsTrends], 
                            cycleMetricsTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
                                metrics: [
                                   min_latency
                                   max_latency
                                   avg_latency
                                   percentile_latency 
                                ],
                                latencyTargetPercentile: $percentile,
                            }

                        ) {
                            cycleMetricsTrends {
                                measurementDate
                                measurementWindow
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
            days=30,
            window=10,
            sample=10,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert len(project['cycleMetricsTrends']) == 4
        for index, measurement in enumerate(project['cycleMetricsTrends']):
            if index == 0:
                assert measurement['minLatency'] == 1.0
                assert measurement['avgLatency'] == 3.0
                assert measurement['maxLatency'] == 5.0
                assert measurement['percentileLatency'] == 4.0

            else:
                assert not measurement['minLatency']
                assert not measurement['avgLatency']
                assert not measurement['maxLatency']
                assert not measurement['percentileLatency']

    def it_filters_epics_and_includes_subtasks_by_default(self, api_work_items_import_fixture):
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
            api_helper.update_work_items([(i, 'closed', start_date + timedelta(days=1))])

        client = Client(schema)
        query = """
                query getProjectCycleMetricsTrends(
                    $project_key:String!, 
                    $days: Int!, 
                    $window: Int!,
                    $sample: Int,
                    $percentile: Float
                ) {
                    project(
                        key: $project_key, 
                        interfaces: [CycleMetricsTrends], 
                        cycleMetricsTrendsArgs: {
                            days: $days,
                            measurementWindow: $window,
                            samplingFrequency: $sample,
                            metrics: [
                                work_items_in_scope
                            ],
                            leadTimeTargetPercentile: $percentile,
                            cycleTimeTargetPercentile: $percentile
                        }

                    ) {
                        cycleMetricsTrends {
                            workItemsInScope
                        }
                    }
                }
            """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            days=30,
            window=10,
            sample=10,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert len(project['cycleMetricsTrends']) == 4
        for index, measurement in enumerate(project['cycleMetricsTrends']):
            if index == 0:
                # subtask and regular item should be reported
                assert measurement['workItemsInScope'] == 2

            else:
                assert not measurement['workItemsInScope']

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

        work_items[0]['is_epic'] = True
        work_items[1]['work_item_type'] = 'subtask'

        api_helper.import_work_items(work_items)

        for i in range(0, 3):
            api_helper.update_work_items([(i, 'upnext', start_date + timedelta(days=1))])
            api_helper.update_work_items([(i, 'closed', start_date + timedelta(days=1))])

        client = Client(schema)
        query = """
                query getProjectCycleMetricsTrends(
                    $project_key:String!, 
                    $days: Int!, 
                    $window: Int!,
                    $sample: Int,
                    $percentile: Float
                ) {
                    project(
                        key: $project_key, 
                        interfaces: [CycleMetricsTrends], 
                        cycleMetricsTrendsArgs: {
                            days: $days,
                            measurementWindow: $window,
                            samplingFrequency: $sample,
                            metrics: [
                                work_items_in_scope
                            ],
                            leadTimeTargetPercentile: $percentile,
                            cycleTimeTargetPercentile: $percentile, 
                            includeEpics: true,
                            includeSubTasks: false
                        }

                    ) {
                        cycleMetricsTrends {
                            workItemsInScope
                        }
                    }
                }
            """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            days=30,
            window=10,
            sample=10,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert len(project['cycleMetricsTrends']) == 4
        for index, measurement in enumerate(project['cycleMetricsTrends']):
            if index == 0:
                assert measurement['workItemsInScope'] == 2

            else:
                assert not measurement['workItemsInScope']

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
            api_helper.update_work_items([(i, 'closed', start_date + timedelta(days=1))])

        client = Client(schema)
        query = """
                query getProjectCycleMetricsTrends(
                    $project_key:String!, 
                    $days: Int!, 
                    $window: Int!,
                    $sample: Int,
                    $percentile: Float
                ) {
                    project(
                        key: $project_key, 
                        interfaces: [CycleMetricsTrends], 
                        cycleMetricsTrendsArgs: {
                            days: $days,
                            measurementWindow: $window,
                            samplingFrequency: $sample,
                            metrics: [
                                work_items_in_scope
                            ],
                            leadTimeTargetPercentile: $percentile,
                            cycleTimeTargetPercentile: $percentile, 
                            defectsOnly: true
                        }

                    ) {
                        cycleMetricsTrends {
                            workItemsInScope
                        }
                    }
                }
            """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            days=30,
            window=10,
            sample=10,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert len(project['cycleMetricsTrends']) == 4
        for index, measurement in enumerate(project['cycleMetricsTrends']):
            if index == 0:
                assert measurement['workItemsInScope'] == 1

            else:
                assert not measurement['workItemsInScope']

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
            api_helper.update_work_items([(i, 'closed', start_date + timedelta(days=5))])

        client = Client(schema)
        query = """
                query getProjectCycleMetricsTrends(
                    $project_key:String!, 
                    $days: Int!, 
                    $window: Int!,
                    $sample: Int,
                    $percentile: Float
                ) {
                    project(
                        key: $project_key, 
                        interfaces: [CycleMetricsTrends], 
                        cycleMetricsTrendsArgs: {
                            days: $days,
                            measurementWindow: $window,
                            samplingFrequency: $sample,
                            metrics: [
                                work_items_in_scope,
                                avg_cycle_time
                            ],
                            leadTimeTargetPercentile: $percentile,
                            cycleTimeTargetPercentile: $percentile, 
                            specsOnly: true
                        }

                    ) {
                        cycleMetricsTrends {
                            workItemsInScope
                            avgCycleTime
                        }
                    }
                }
            """
        result = client.execute(query, variable_values=dict(
            project_key=project.key,
            days=30,
            window=10,
            sample=10,
            percentile=0.70
        ))
        assert result['data']
        project = result['data']['project']
        # we expect one measurement for each point in the window including the end points.
        assert len(project['cycleMetricsTrends']) == 4
        for index, measurement in enumerate(project['cycleMetricsTrends']):
            if index == 0:
                assert measurement['workItemsInScope'] == 1
                assert measurement['avgCycleTime'] == 3.0
            else:
                assert not measurement['workItemsInScope']
