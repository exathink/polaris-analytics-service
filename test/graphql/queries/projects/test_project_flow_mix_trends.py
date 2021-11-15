from graphene.test import Client
from polaris.analytics.db.enums import PivotalTrackerWorkItemType, \
    WorkItemTypesToFlowTypes, \
    all_work_item_types, \
    num_task_types, \
    num_defect_types, \
    num_feature_types

from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import *

from collections import OrderedDict

from test.graphql.queries.projects.shared_testing_mixins import \
    TrendingWindowTestNumberOfMeasurements, \
    TrendingWindowMeasurementDate


class TestProjectFlowMixTrends:

    @pytest.fixture
    def setup(self, api_work_items_import_fixture):
        organization, project, work_items_source, work_items_common = api_work_items_import_fixture
        api_helper = WorkItemImportApiHelper(organization, work_items_source)

        yield Fixture(
            project=project,
            api_helper=api_helper,
            work_items_common=work_items_common,
            feature_type_count=len(WorkItemTypesToFlowTypes.feature_types),
            task_type_count=len(WorkItemTypesToFlowTypes.task_types)
        )

    class TestMeasurementWindowContracts(
        TrendingWindowTestNumberOfMeasurements,
        TrendingWindowMeasurementDate
    ):

        @pytest.fixture
        def setup(self, setup):
            fixture = setup

            start_date = datetime.utcnow() - timedelta(days=10)

            work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name=f'Issue {i}',
                    display_id='1000',
                    state='backlog',
                    created_at=start_date,
                    updated_at=start_date,

                    **fixture.work_items_common
                )
                for i in range(0, len(all_work_item_types))
            ]

            measurements_query = """
                query getProjectFlowMixTrends(
                    $project_key:String!, 
                    $days: Int!, 
                    $window: Int!,
                    $sample: Int,
                ) {
                    project(
                        key: $project_key, 
                        interfaces: [FlowMixTrends], 
                        flowMixTrendsArgs: {
                            days: $days,
                            measurementWindow: $window,
                            samplingFrequency: $sample,
                        }

                    ) {
                        flowMixTrends {
                            measurementDate
                            measurementWindow
                        }
                    }
                }
            """

            yield Fixture(
                parent=fixture,
                work_items=work_items,
                start_date=start_date,
                query=measurements_query,
                output_attribute='flowMixTrends'
            )

    class TestFlowMixCalculations:
        @pytest.fixture
        def setup(self, setup):
            fixture = setup

            start_date = datetime.utcnow() - timedelta(days=10)

            # import them all in a closed state for convenience
            work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name=f'Issue {i}',
                    display_id='1000',
                    state='closed',
                    created_at=start_date,
                    updated_at=start_date + timedelta(days=1),
                    **fixture.work_items_common
                )
                for i in range(0, len(all_work_item_types))
            ]

            api_helper = fixture.api_helper
            api_helper.import_work_items(work_items)

            measurements_query = """
                            query getProjectFlowMixTrends(
                                $project_key:String!, 
                                $days: Int!, 
                                $window: Int!,
                                $sample: Int,
                                $specsOnly: Boolean
                            ) {
                                project(
                                    key: $project_key, 
                                    interfaces: [FlowMixTrends], 
                                    flowMixTrendsArgs: {
                                        days: $days,
                                        measurementWindow: $window,
                                        samplingFrequency: $sample,
                                        specsOnly: $specsOnly,
                                    }

                                ) {
                                    flowMixTrends {
                                        flowMix {
                                            category
                                            workItemCount
                                            totalEffort
                                        }
                                    }
                                }
                            }
                        """

            yield Fixture(
                parent=fixture,
                work_items=work_items,
                start_date=start_date,
                query=measurements_query,
            )

        class WhenThereAreNoSpecs:

            def it_computes_the_flow_mix_correctly(self, setup):
                fixture = setup
                api_helper = fixture.api_helper
                # Update the work_item_types of the work items so that there is one of each type
                with db.orm_session() as session:
                    # We will create one of
                    # each flow type
                    for index, work_item_type in enumerate(all_work_item_types):
                        api_helper.update_work_item_attributes(
                            index, dict(work_item_type=work_item_type, is_bug=False), join_this=session
                        )

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=30,
                    window=30,
                    sample=30,
                ))
                assert result['data']
                project = result['data']['project']
                current, _ = project['flowMixTrends']
                # we have on of each type
                assert current['flowMix'] == [
                    OrderedDict(
                        [('category', 'defect'), ('workItemCount', float(num_defect_types)), ('totalEffort', None)]),
                    OrderedDict(
                        [('category', 'feature'), ('workItemCount', float(num_feature_types)), ('totalEffort', None)]),
                    OrderedDict(
                        [('category', 'task'), ('workItemCount', float(num_task_types)), ('totalEffort', None)])]

            def it_respects_the_is_bug_flag_on_a_work_item_to_override_the_work_item_type(self, setup):
                fixture = setup
                api_helper = fixture.api_helper
                # Update the work_item_types of the work items so that there is one of each type
                with db.orm_session() as session:
                    # We will create one of
                    # each flow type
                    for index, work_item_type in enumerate(all_work_item_types):
                        api_helper.update_work_item_attributes(
                            index, dict(work_item_type=work_item_type, is_bug=True), join_this=session
                        )

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=30,
                    window=30,
                    sample=30,
                ))
                assert result['data']
                project = result['data']['project']
                current, _ = project['flowMixTrends']
                # we have on of each type
                assert current['flowMix'] == [
                    OrderedDict(
                        [('category', 'defect'), ('workItemCount', len(all_work_item_types)), ('totalEffort', None)])]

            def it_reports_only_the_total_work_items_even_if_there_are_multiple_delivery_cycles_for_some(self, setup):
                fixture = setup
                api_helper = fixture.api_helper

                # re-open and close the first work item so it has two delivery cycles.
                # should not change the work item count.
                api_helper.update_work_items([(0, 'upnext', fixture.start_date + timedelta(days=3))])
                api_helper.update_work_items([(0, 'closed', fixture.start_date + timedelta(days=4))])

                # Update the work_item_types of the work items so that there is one of each type
                with db.orm_session() as session:
                    # We will create one of
                    # each flow type
                    for index, work_item_type in enumerate(all_work_item_types):
                        api_helper.update_work_item_attributes(
                            index, dict(work_item_type=work_item_type, is_bug=False), join_this=session

                        )

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=30,
                    window=30,
                    sample=30,
                ))
                assert result['data']
                project = result['data']['project']
                current, _ = project['flowMixTrends']
                # we have on of each type
                assert current['flowMix'] == [
                    OrderedDict([('category', 'defect'), ('workItemCount', num_defect_types), ('totalEffort', None)]),
                    OrderedDict([('category', 'feature'), ('workItemCount', num_feature_types), ('totalEffort', None)]),
                    OrderedDict([('category', 'task'), ('workItemCount', num_task_types), ('totalEffort', None)])]

        class WhenThereAreSpecs:

            @pytest.fixture
            def setup(self, setup):
                fixture = setup
                api_helper = fixture.api_helper
                # Update the work_item_types of the work items so that there is one of each type
                with db.orm_session() as session:
                    # We will create one of
                    # each flow type
                    for index, work_item_type in enumerate(all_work_item_types):
                        api_helper.update_work_item_attributes(
                            index, dict(work_item_type=work_item_type, is_bug=False), join_this=session
                        )

                yield fixture

            def it_reports_total_effort_for_specs(self, setup):
                fixture = setup
                api_helper = fixture.api_helper
                # set up all as specs
                for i in range(0, len(all_work_item_types)):
                    api_helper.update_delivery_cycles([(i, dict(property='effort', value=1))])
                    api_helper.update_delivery_cycles([(i, dict(property='commit_count', value=1))])

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=30,
                    window=30,
                    sample=30,
                ))
                assert result['data']
                project = result['data']['project']
                current, _ = project['flowMixTrends']

                assert current['flowMix'] == [
                    OrderedDict(
                        [('category', 'defect'), ('workItemCount', 2.0), ('totalEffort', float(num_defect_types))]),
                    OrderedDict(
                        [('category', 'feature'), ('workItemCount', 4.0), ('totalEffort', float(num_feature_types))]),
                    OrderedDict([('category', 'task'), ('workItemCount', 7.0), ('totalEffort', float(num_task_types))])
                ]

        class TestParameters:
            @pytest.fixture
            def setup(self, setup):
                fixture = setup
                api_helper = fixture.api_helper
                # Update the work_item_types of the work items so that there is one of each type
                with db.orm_session() as session:
                    # We will create one of
                    # each flow type
                    for index, work_item_type in enumerate(all_work_item_types):
                        api_helper.update_work_item_attributes(
                            index, dict(work_item_type=work_item_type, is_bug=False), join_this=session
                        )

                yield fixture

            def it_respects_the_specs_only_parameter(self, setup):
                fixture = setup
                api_helper = fixture.api_helper

                # Make a single task flow type a spec. All others are non specs.
                # Picking Pivotal chore, since its name is not likely to conflict with other providers.
                # keeping the test stable even if we add types later. Will break if another 'chore' is added

                for i in range(0, len(all_work_item_types)):
                    if all_work_item_types[i] == PivotalTrackerWorkItemType.chore.value:
                        api_helper.update_delivery_cycles([(i, dict(property='effort', value=1))])
                        api_helper.update_delivery_cycles([(i, dict(property='commit_count', value=1))])

                client = Client(schema)

                result = client.execute(fixture.query, variable_values=dict(
                    project_key=fixture.project.key,
                    days=30,
                    window=30,
                    sample=30,
                    specsOnly=True
                ))
                assert result['data']
                project = result['data']['project']
                current, _ = project['flowMixTrends']

                assert current['flowMix'] == [
                    OrderedDict([('category', 'task'), ('workItemCount', 1.0), ('totalEffort', 1.0)])
                ]
