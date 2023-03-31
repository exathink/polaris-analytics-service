# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from graphene.test import Client
from polaris.analytics.db.enums import JiraWorkItemType
from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *

from test.graphql.queries.projects.shared_testing_mixins import \
    TrendingWindowTestNumberOfMeasurements, \
    TrendingWindowMeasurementDate


class TestProjectResponseTimePredictabilityTrends:

    @pytest.fixture
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

    class TestNumberOfMeasurements(
        TrendingWindowTestNumberOfMeasurements,
        TrendingWindowMeasurementDate
    ):

        @pytest.fixture
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
                            measurementWindow
                        }
                    }
                }
            """

            yield Fixture(
                parent=fixture,
                query=measurements_query,
                output_attribute='responseTimeConfidenceTrends'
            )

    class TestResponseTimeConfidenceCalculations:

        @pytest.fixture
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
            @pytest.fixture
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
                           dict(l=1, c=0),
                           dict(l=0, c=0)
                       ]

            def it_reports_lead_time_confidence_of_0_when_target_is_smaller_than_all_lead_times(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=10,
                    window=10,
                    sample=10,
                    lead_time_target=0,
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
                           dict(l=0, c=0)
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
                           dict(l=0.6666666666666666, c=0.0),
                           dict(l=0.0, c=0.0)

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
                           dict(l=0.6666666666666666, c=0),
                           dict(l=0.0, c=0.0)
                       ]

        class CaseWorkItemsWithLeadTimesAndCycleTimes:
            @pytest.fixture
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
                           dict(l=1, c=1),
                           dict(l=0, c=0),

                       ]

            def it_reports_cycle_time_confidence_of_0_when_target_is_smaller_than_all_cycle_times(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=10,
                    window=10,
                    sample=10,
                    lead_time_target=10,
                    # target < all cycle times
                    cycle_time_target=0,
                ))
                assert result['data']
                project = result['data']['project']

                assert len(project['responseTimeConfidenceTrends']) == 2
                assert [
                           dict(l=measurement['leadTimeConfidence'], c=measurement['cycleTimeConfidence'])
                           for measurement in project['responseTimeConfidenceTrends']
                       ] == [
                           dict(l=1, c=0),
                           dict(l=0, c=0),

                       ]

            def it_reports_nonzero_cycle_time_confidence_when_target_is_equal_to_the_smallest_cycle_time(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=10,
                    window=10,
                    sample=10,
                    lead_time_target=10,
                    # target = min(all cycle times)
                    cycle_time_target=1,
                ))
                assert result['data']
                project = result['data']['project']

                assert len(project['responseTimeConfidenceTrends']) == 2
                assert [
                           dict(l=measurement['leadTimeConfidence'], c=measurement['cycleTimeConfidence'])
                           for measurement in project['responseTimeConfidenceTrends']
                       ] == [
                           dict(l=1.0, c=0.3333333333333333),
                           dict(l=0.0, c=0.0),

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
                           dict(l=1.0, c=0.6666666666666666),
                           dict(l=0.0, c=0.0),

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
                           dict(l=1.0, c=0.6666666666666666),
                           dict(l=0.0, c=0.0),

                       ]

        class CaseWorkItemsWithLeadTimesAndSomeNullCycleTimes:
            @pytest.fixture
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

            def it_reports_cycle_time_confidence_1_when_target_is_greater_than_all_cycle_times(self, setup):
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
                           dict(l=1, c=1),
                           dict(l=0, c=0),

                       ]

    class TestParameters:

        class TestSpecsOnly:

            @pytest.fixture
            def setup(self, setup):
                fixture = setup

                api_helper = fixture.api_helper
                api_helper.import_work_items(fixture.work_items)

                start_date = fixture.start_date
                # cycle time = 1
                api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
                api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=2))])

                # cycle time = 2
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
                                        specsOnly: $specs_only
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
                           dict(l=1, c=1),
                           dict(l=0, c=0),
                       ]

        class TestEpicsAndSubTasks:
            @pytest.fixture
            def setup(self, setup):
                fixture = setup

                api_helper = fixture.api_helper

                # set up epics and subtasks
                fixture.work_items[0]['is_epic'] = True
                fixture.work_items[1]['work_item_type'] = 'subtask'

                api_helper.import_work_items(fixture.work_items)

                start_date = fixture.start_date
                # cycle time = 5
                api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=1))])
                api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=5))])

                # cycle time = 5
                api_helper.update_work_items([(1, 'upnext', start_date + timedelta(days=3))])
                api_helper.update_work_items([(1, 'closed', start_date + timedelta(days=8))])

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
                            $include_epics: Boolean,
                            $include_sub_tasks: Boolean,

                        ) {
                            project(
                                key: $project_key, 
                                interfaces: [ResponseTimeConfidenceTrends], 
                                responseTimeConfidenceTrendsArgs: {
                                    days: $days,
                                    measurementWindow: $window,
                                    samplingFrequency: $sample,
                                    cycleTimeTarget : $cycle_time_target,
                                    leadTimeTarget: $lead_time_target,
                                    includeEpics: $include_epics,
                                    includeSubTasks: $include_sub_tasks
                                    
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
                    query=query
                )

            def test_excludes_epics_and_subtasks(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=10,
                    window=10,
                    sample=10,
                    lead_time_target=10,
                    # set cycle time target = 4 epic and subtask have cycle time = 5 so cycle time confidence is < 1
                    # when they are included.
                    cycle_time_target=3,
                    include_epics=False,
                    include_sub_tasks=False
                ))
                assert result['data']
                project = result['data']['project']

                assert len(project['responseTimeConfidenceTrends']) == 2
                assert [
                           dict(l=measurement['leadTimeConfidence'], c=measurement['cycleTimeConfidence'])
                           for measurement in project['responseTimeConfidenceTrends']
                       ] == [
                           dict(l=1, c=1),

                           dict(l=0, c=0)
                       ]

            def test_excludes_epics_and_include_subtasks(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=10,
                    window=10,
                    sample=10,
                    lead_time_target=10,
                    # set cycle time target = 4 epic and subtask have cycle time = 5 so cycle time confidence is < 1
                    # when they are included.
                    cycle_time_target=3,
                    include_epics=False,
                    include_sub_tasks=True
                ))
                assert result['data']
                project = result['data']['project']

                assert len(project['responseTimeConfidenceTrends']) == 2
                assert [
                           dict(l=measurement['leadTimeConfidence'], c=measurement['cycleTimeConfidence'])
                           for measurement in project['responseTimeConfidenceTrends']
                       ] == [
                           dict(l=1, c=0.5),

                           dict(l=0, c=0)
                       ]

            def test_includes_epics_and_excludes_subtasks(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=10,
                    window=10,
                    sample=10,
                    lead_time_target=10,
                    # set cycle time target = 4 epic and subtask have cycle time = 5 so cycle time confidence is < 1
                    # when they are included.
                    cycle_time_target=3,
                    include_epics=True,
                    include_sub_tasks=False
                ))
                assert result['data']
                project = result['data']['project']

                assert len(project['responseTimeConfidenceTrends']) == 2
                assert [
                           dict(l=measurement['leadTimeConfidence'], c=measurement['cycleTimeConfidence'])
                           for measurement in project['responseTimeConfidenceTrends']
                       ] == [
                           dict(l=1, c=0.5),

                           dict(l=0, c=0)
                       ]

            def test_includes_epics_and_subtasks(self, setup):
                fixture = setup

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=10,
                    window=10,
                    sample=10,
                    lead_time_target=10,
                    # set cycle time target = 4 epic and subtask have cycle time = 5 so cycle time confidence is < 1
                    # when they are included.
                    cycle_time_target=3,
                    include_epics=True,
                    include_sub_tasks=True
                ))
                assert result['data']
                project = result['data']['project']

                assert len(project['responseTimeConfidenceTrends']) == 2
                assert [
                           dict(l=measurement['leadTimeConfidence'], c=measurement['cycleTimeConfidence'])
                           for measurement in project['responseTimeConfidenceTrends']
                       ] == [
                           dict(l=1.0, c=0.3333333333333333),

                           dict(l=0.0, c=0.0)
                       ]
