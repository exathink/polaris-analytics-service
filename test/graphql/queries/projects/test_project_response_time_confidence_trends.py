# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from graphene.test import Client
from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *


class TestProjectResponseTimePredictabilityTrends:

    @pytest.yield_fixture
    def setup(self, api_work_items_import_fixture):
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

        yield Fixture(
            project=project,
            api_helper=api_helper,
            work_items=work_items,
            start_date=start_date,
        )

    class TestNumberOfMeasurements:

        @pytest.yield_fixture
        def setup(self, setup):
            fixture = setup
            measurements_query = """
                query getProjectPredictabilityTrends(
                    $project_key:String!, 
                    $days: Int!, 
                    $window: Int!,
                    $sample: Int,
                ) {
                    project(
                        key: $project_key, 
                        interfaces: [ResponseTimeConfidenceTrends], 
                        responseTimeConfidenceTrendsArgs: {
                            days: $days,
                            measurementWindow: $window,
                            samplingFrequency: $sample,
                            cycleTimeTarget : 7
                            leadTimeTarget: 14
                        }

                    ) {
                        responseTimeConfidenceTrends {
                            measurementDate
                        }
                    }
                }
            """

            yield Fixture(
                parent=fixture,
                query=measurements_query,
            )

        class WhenThereNoWorkItems:
            def it_returns_a_sample_for_each_day_when_there_when_there_are_no_work_items(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=30,
                    window=7,
                    sample=1
                ))
                assert result['data']
                project = result['data']['project']
                # we expect one measurement for each point in the window including the end points.
                assert len(project['responseTimeConfidenceTrends']) == 31

            def it_returns_a_sample_for_each_day_when_there_are_no_CLOSED_work_items(self, setup):
                fixture = setup

                fixture.api_helper.import_work_items(fixture.work_items)

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=30,
                    window=7,
                    sample=1
                ))
                assert result['data']
                project = result['data']['project']
                # we expect one measurement for each point in the window including the end points.
                assert len(project['responseTimeConfidenceTrends']) == 31

        class WhenThereAreClosedWorkItems:

            @pytest.yield_fixture
            def setup(self, setup):
                fixture = setup

                api_helper = fixture.api_helper
                api_helper.import_work_items(fixture.work_items)

                start_date = fixture.start_date

                api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=1))])
                api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=2))])

                yield fixture

            def test_daily_samples(self, setup):
                fixture = setup

                client = Client(schema)

                # sampling frequency = 1 days = 30 means 31 samples expected including endpoints
                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=30,
                    window=7,
                    sample=1
                ))
                assert result['data']
                project = result['data']['project']
                # we expect one measurement for each point in the window including the end points.
                assert len(project['responseTimeConfidenceTrends']) == 31

            def test_weekly_sample_frequency(self, setup):
                fixture = setup

                client = Client(schema)

                # sampling frequency = 7 days = 30 means 5 samples expected including endpoints
                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=30,
                    window=7,
                    sample=7
                ))
                assert result['data']
                project = result['data']['project']
                # we expect one measurement for each point in the window including the end points.
                assert len(project['responseTimeConfidenceTrends']) == 5

            def test_changing_window_has_no_impact_on_number_of_measurements(self, setup):
                fixture = setup

                client = Client(schema)

                # Same as last test with different window. Should have no impact.

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=30,
                    window=14,
                    sample=7
                ))
                assert result['data']
                project = result['data']['project']
                # we expect one measurement for each point in the window including the end points.
                assert len(project['responseTimeConfidenceTrends']) == 5

            def test_current_previous_window_behavior(self, setup):
                fixture = setup

                client = Client(schema)

                # This tests the window setting for showing current week vs previous. Used
                # in the main app dashboard so adding a test for this case.

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=7,
                    window=30,
                    sample=7
                ))
                assert result['data']
                project = result['data']['project']
                # we expect one measurement for each point in the window including the end points.
                assert len(project['responseTimeConfidenceTrends']) == 2

    class TestResponseTimeConfidenceCalculations:

        @pytest.yield_fixture
        def setup(self, setup):
            fixture = setup
            query = """
                        query getProjectPredictabilityTrends(
                            $project_key:String!, 
                            $days: Int!, 
                            $window: Int!,
                            $cycle_time_target: Int!
                            $lead_time_target: Int!
                            $sample: Int,
                            
                        ) {
                            project(
                                key: $project_key, 
                                interfaces: [ResponseTimeConfidenceTrends], 
                                responseTimeConfidenceTrendsArgs: {
                                    days: $days,
                                    measurementWindow: $window,
                                    samplingFrequency: $sample,
                                    cycleTimeTarget : $cycle_time_target
                                    leadTimeTarget: $lead_time_target
                                }

                            ) {
                                responseTimeConfidenceTrends {
                                    measurementDate
                                    measurementWindow
                                    leadTimeTarget
                                    leadTimeConfidence
                                    cycleTimeTarget
                                    cycleTimeConfidence
                                }
                            }
                        }
                    """

            yield Fixture(
                parent=fixture,
                query=query,
            )

        class WhenThereNoWorkItems:
            def it_returns_a_confidence_value_of_zero_for_each_measurement(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=30,
                    window=7,
                    sample=1,
                    cycle_time_target=7,
                    lead_time_target=14,
                ))
                assert result['data']
                project = result['data']['project']
                # we expect one measurement for each point in the window including the end points.
                assert len(project['responseTimeConfidenceTrends']) == 31
                for measurement in project['responseTimeConfidenceTrends']:
                    assert measurement['cycleTimeConfidence'] == 0
                    assert measurement['leadTimeConfidence'] == 0

            def it_returns_the_measurement_window_and_targets(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=30,
                    window=7,
                    sample=1,
                    cycle_time_target=7,
                    lead_time_target=14,
                ))
                assert result['data']
                project = result['data']['project']
                # we expect one measurement for each point in the window including the end points.
                assert len(project['responseTimeConfidenceTrends']) == 31
                for measurement in project['responseTimeConfidenceTrends']:
                    assert measurement['measurementWindow'] == 7
                    assert measurement['cycleTimeTarget'] == 7
                    assert measurement['leadTimeTarget'] == 14

        class CaseWorkItemsWithLeadTimesButNoCycleTimes:
            @pytest.yield_fixture
            def setup(self, setup):
                fixture = setup

                api_helper = fixture.api_helper
                api_helper.import_work_items(fixture.work_items)

                start_date = fixture.start_date
                api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=1))])
                api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=5))])
                api_helper.update_work_items([(2, 'closed', start_date + timedelta(days=8))])

                yield fixture

            def it_reports_lead_time_confidence_of_1_when_target_is_greater_than_all_lead_times(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=10,
                    window=10,
                    sample=10,
                    lead_time_target=10,
                    cycle_time_target=7,
                ))
                assert result['data']
                project = result['data']['project']

                assert len(project['responseTimeConfidenceTrends']) == 2
                assert [
                           dict(l=measurement['leadTimeConfidence'], c=measurement['cycleTimeConfidence'])
                           for measurement in project['responseTimeConfidenceTrends']
                       ] == [
                           dict(l=0, c=0),
                           dict(l=1, c=0)
                       ]

            def it_reports_lead_time_confidence_correctly_when_target_is_not_greater_than_all_lead_times(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=10,
                    window=10,
                    sample=10,
                    lead_time_target=7,
                    cycle_time_target=7,
                ))
                assert result['data']
                project = result['data']['project']

                assert len(project['responseTimeConfidenceTrends']) == 2
                assert [
                           dict(l=measurement['leadTimeConfidence'], c=measurement['cycleTimeConfidence'])
                           for measurement in project['responseTimeConfidenceTrends']
                       ] == [
                           dict(l=0, c=0),
                           dict(l=0.666666666666667, c=0)
                       ]

            def it_reports_lead_time_confidence_correctly_when_target_is_equal_to_some_lead_times(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=10,
                    window=10,
                    sample=10,
                    lead_time_target=5,
                    cycle_time_target=7,
                ))
                assert result['data']
                project = result['data']['project']

                assert len(project['responseTimeConfidenceTrends']) == 2
                assert [
                           dict(l=measurement['leadTimeConfidence'], c=measurement['cycleTimeConfidence'])
                           for measurement in project['responseTimeConfidenceTrends']
                       ] == [
                           dict(l=0, c=0),
                           # In this case the lead time target is equal to one of the lead times.
                           # We are reporting the number of values strictly smaller than the target value
                           # and so we actually dont report the value that is exactly equal to the target.
                           # The only scenario where this really matters is there is a value whose
                           # lead/cycle time matches the target value down to the second, and in this
                           # case our answer is technically wrong. This test case documents that scenario.
                           # In most practical situations this will not really matter I suspect, and the convolutions
                           # we need to make in the code to make that work correctly are really complicated, so
                           # I am choosing to leave this "error" in. If it becomes a practical problem for
                           # some reason we will revisit it.
                           dict(l=0.333333333333333, c=0)
                       ]

        class CaseWorkItemsWithLeadTimesAndCycleTimes:
            @pytest.yield_fixture
            def setup(self, setup):
                fixture = setup

                api_helper = fixture.api_helper
                api_helper.import_work_items(fixture.work_items)

                start_date = fixture.start_date
                # cycle time = 1
                api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
                api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=2))])
                # cycle_time = 2
                api_helper.update_work_items([(1, 'upnext', start_date + timedelta(days=5))])
                api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=7))])

                # cycle_time = 4
                api_helper.update_work_items([(2, 'upnext', start_date + timedelta(days=5))])
                api_helper.update_work_items([(2, 'closed', start_date + timedelta(days=9))])

                yield fixture

            def it_reports_cycle_time_confidence_of_1_when_target_is_greater_than_all_cycle_times(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=10,
                    window=10,
                    sample=10,
                    lead_time_target=10,
                    # target > all cycle times
                    cycle_time_target=5,
                ))
                assert result['data']
                project = result['data']['project']

                assert len(project['responseTimeConfidenceTrends']) == 2
                assert [
                           dict(l=measurement['leadTimeConfidence'], c=measurement['cycleTimeConfidence'])
                           for measurement in project['responseTimeConfidenceTrends']
                       ] == [
                           dict(l=0, c=0),
                           dict(l=1, c=1)
                       ]

            def it_reports_cycle_time_confidence_correctly_when_target_is_not_greater_than_all_cycle_times(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=10,
                    window=10,
                    sample=10,
                    lead_time_target=10,
                    # target > all cycle times
                    cycle_time_target=3,
                ))
                assert result['data']
                project = result['data']['project']

                assert len(project['responseTimeConfidenceTrends']) == 2
                assert [
                           dict(l=measurement['leadTimeConfidence'], c=measurement['cycleTimeConfidence'])
                           for measurement in project['responseTimeConfidenceTrends']
                       ] == [
                           dict(l=0, c=0),
                           dict(l=1, c=0.666666666666667)
                       ]

            def it_reports_cycle_time_confidence_correctly_when_target_is_equal_to_some_cycle_times(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=10,
                    window=10,
                    sample=10,
                    lead_time_target=10,
                    # target > all cycle times
                    cycle_time_target=2,
                ))
                assert result['data']
                project = result['data']['project']

                assert len(project['responseTimeConfidenceTrends']) == 2
                assert [
                           dict(l=measurement['leadTimeConfidence'], c=measurement['cycleTimeConfidence'])
                           for measurement in project['responseTimeConfidenceTrends']
                       ] == [
                           dict(l=0, c=0),
                           dict(l=1, c=0.333333333333333)
                       ]

        class CaseWorkItemsWithLeadTimesAndSomeNullCycleTimes:
            @pytest.yield_fixture
            def setup(self, setup):
                fixture = setup

                api_helper = fixture.api_helper
                api_helper.import_work_items(fixture.work_items)

                start_date = fixture.start_date
                # cycle time = 1
                api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
                api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=2))])
                # cycle_time = 2
                api_helper.update_work_items([(1, 'upnext', start_date + timedelta(days=5))])
                api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=7))])

                # cycle_time = null
                api_helper.update_work_items([(2, 'closed', start_date + timedelta(days=9))])

                yield fixture

            def it_reports_cycle_time_confidence_less_than_1_when_target_is_greater_than_all_cycle_times(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=10,
                    window=10,
                    sample=10,
                    lead_time_target=10,
                    cycle_time_target=7,
                ))
                assert result['data']
                project = result['data']['project']

                assert len(project['responseTimeConfidenceTrends']) == 2
                assert [
                           dict(l=measurement['leadTimeConfidence'], c=measurement['cycleTimeConfidence'])
                           for measurement in project['responseTimeConfidenceTrends']
                       ] == [
                           dict(l=0, c=0),
                           # This is another behavior that is counter-intuitive initially, but worth
                           # calling out in a test. When there are null values in the cycle times, which
                           # is often possible for items that dont move through the delivery cycle, (but very unlikely
                           # for specs in steady state, our confidence reporting assumes those null values exist, and
                           # a reports a confidence of less than 1 even if the  target is less than all non-null values.
                           # This is a more conservative strategy than reporting a confidence of 1 in this case.
                           # However, a side effect of this is that we report confidence < 1 even though the target
                           # is greater than the Max cycle value reported via CycleMetricsTrends.
                           dict(l=1, c=0.666666666666667)
                       ]

    class TestParameters:

        class TestSpecsOnly:
            @pytest.yield_fixture
            def setup(self, setup):
                fixture = setup

                api_helper = fixture.api_helper
                api_helper.import_work_items(fixture.work_items)

                start_date = fixture.start_date
                # cycle time = 1
                api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
                api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=2))])

                #cycle time = 2
                api_helper.update_work_items([(1, 'upnext', start_date + timedelta(days=3))])
                api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=5))])

                # cycle time = 3
                api_helper.update_work_items([(2, 'upnext', start_date + timedelta(days=5))])
                api_helper.update_work_items([(2, 'closed', start_date + timedelta(days=8))])

                query = """
                            query getProjectPredictabilityTrends(
                                $project_key:String!, 
                                $days: Int!, 
                                $window: Int!,
                                $cycle_time_target: Int!
                                $lead_time_target: Int!
                                $sample: Int,
                                $specs_only: Boolean
    
                            ) {
                                project(
                                    key: $project_key, 
                                    interfaces: [ResponseTimeConfidenceTrends], 
                                    responseTimeConfidenceTrendsArgs: {
                                        days: $days,
                                        measurementWindow: $window,
                                        samplingFrequency: $sample,
                                        cycleTimeTarget : $cycle_time_target
                                        leadTimeTarget: $lead_time_target
                                    }
                                    specsOnly: $specs_only
                                ) {
                                    responseTimeConfidenceTrends {
                                        measurementDate
                                        measurementWindow
                                        leadTimeTarget
                                        leadTimeConfidence
                                        cycleTimeTarget
                                        cycleTimeConfidence
                                    }
                                }
                            }
                        """
                yield Fixture(
                    parent=fixture,
                    query=query
                )

            def it_respects_specs_only_parameter_when_there_are_no_specs(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=10,
                    window=10,
                    sample=10,
                    lead_time_target=10,
                    cycle_time_target=7,
                    specs_only=True
                ))
                assert result['data']
                project = result['data']['project']

                assert len(project['responseTimeConfidenceTrends']) == 2
                assert [
                           dict(l=measurement['leadTimeConfidence'], c=measurement['cycleTimeConfidence'])
                           for measurement in project['responseTimeConfidenceTrends']
                       ] == [
                           dict(l=0, c=0),

                           dict(l=0, c=0)
                       ]

            def it_respects_specs_only_parameter_when_there_are_specs(self, setup):
                fixture = setup

                api_helper = fixture.api_helper

                # two specs, one is not.
                api_helper.update_delivery_cycles([(0, dict(property='commit_count', value=3))])
                api_helper.update_delivery_cycles([(1, dict(property='commit_count', value=3))])

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=10,
                    window=10,
                    sample=10,
                    lead_time_target=10,
                    cycle_time_target=7,
                    specs_only=True
                ))
                assert result['data']
                project = result['data']['project']

                assert len(project['responseTimeConfidenceTrends']) == 2
                assert [
                           dict(l=measurement['leadTimeConfidence'], c=measurement['cycleTimeConfidence'])
                           for measurement in project['responseTimeConfidenceTrends']
                       ] == [
                           dict(l=0, c=0),

                           dict(l=1, c=1)
                       ]
