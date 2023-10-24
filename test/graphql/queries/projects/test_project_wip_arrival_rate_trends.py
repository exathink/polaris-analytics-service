# -*- coding: utf-8 -*-
import datetime

#  Copyright (c) Exathink, LLC  2016-2023.
#  All rights reserved
#

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from graphene.test import Client
from datetime import datetime, timedelta
from polaris.analytics.service.graphql import schema
from polaris.analytics.db import api
from polaris.analytics.db.model import WorkItem
from test.fixtures.project_work_items import *
from polaris.utils.collections import dict_merge
from test.fixtures.graphql import WorkItemImportApiHelper

class TestProjectArrivalDepartureTrends(ProjectWorkItemsTest):
    class TestWipArrivalRate:
        @pytest.fixture()
        def setup(self, setup):
            fixture = setup
            organization = fixture.organization
            work_items_source = fixture.work_items_source
            work_items_common = fixture.work_items_common

            start_date = datetime.utcnow() - timedelta(days=60)
            work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name=f'Issue {i}',
                    display_id=f'ISSUE-{i}',
                    **dict_merge(
                        work_items_common,
                        dict(
                            state='backlog',
                            created_at=start_date,
                            updated_at=start_date + timedelta(days=1)
                        )
                    )
                )
                for i in range(0,10)
            ]
            api_helper = WorkItemImportApiHelper(organization, work_items_source, work_items)


            query = """
                    query getProjectArrivalDepartureTrends(
                        $project_key:String!,
                        $days: Int!,
                        $window: Int!,
                        $sample: Int
                    ) {
                        project(
                            key: $project_key,
                            interfaces: [ArrivalDepartureTrends],
                            arrivalDepartureTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
                            }
                        )
                        {
                            name
                            key
                            arrivalDepartureTrends {
                                measurementDate
                                measurementWindow
                                arrivalRate
                            }
                        }
                    }
                    """


            yield Fixture(
                parent=fixture,
                work_items=work_items,
                start_date=start_date,
                query=query,
                api_helper=api_helper

            )

        def it_returns_zeros_when_there_are_no_arrivals_in_the_period(self, setup):
            fixture = setup
            api_helper = fixture.api_helper
            work_items = fixture.work_items

            client = Client(schema)

            api_helper.import_work_items(work_items)

            result = client.execute(fixture.query, variable_values=dict(
                project_key=fixture.project.key,
                days=30,
                window=30,
                sample=30
            ))
            assert not result.get('errors')
            project = result['data']['project']
            assert len(project['arrivalDepartureTrends']) == 2

            assert [
                0,0
            ] == [
                measurement['arrivalRate']
                for measurement in project['arrivalDepartureTrends']
            ]

        def it_returns_the_arrivals_in_each_period(self, setup):
            fixture = setup
            api_helper = fixture.api_helper
            work_items = fixture.work_items
            start_date = fixture.start_date

            client = Client(schema)

            api_helper.import_work_items(work_items)

            # one arrival in the period start_date, start_date + 30 days
            api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])

            #another arrival in the period start_date + 30 days, start_date + 60 days
            api_helper.update_work_items([(1, 'doing', start_date + timedelta(days=32))])

            result = client.execute(fixture.query, variable_values=dict(
                project_key=fixture.project.key,
                days=30,
                window=30,
                sample=30
            ))
            assert not result.get('errors')
            project = result['data']['project']
            assert len(project['arrivalDepartureTrends']) == 2

            assert  [
                measurement['arrivalRate']
                for measurement in project['arrivalDepartureTrends']
            ] == [
                1,1
            ]

        def it_respects_before_dates_in_the_measurement(self, setup):
            fixture = setup
            api_helper = fixture.api_helper
            work_items = fixture.work_items
            start_date = fixture.start_date

            client = Client(schema)

            api_helper.import_work_items(work_items)

            # one arrival in the period start_date, start_date + 30 days
            api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])

            #another arrival in the period start_date + 30 days, start_date + 60 days
            api_helper.update_work_items([(1, 'doing', start_date + timedelta(days=32))])

            query = """
                    query getProjectArrivalDepartureTrends(
                        $project_key:String!,
                        $before: Date!,
                        $days: Int!,
                        $window: Int!,
                        $sample: Int
                    ) {
                        project(
                            key: $project_key,
                            interfaces: [ArrivalDepartureTrends],
                            arrivalDepartureTrendsArgs: {
                                before: $before,
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
                            }
                        )
                        {
                            name
                            key
                            arrivalDepartureTrends {
                                measurementDate
                                measurementWindow
                                arrivalRate
                            }
                        }
                    }
                    """
            result = client.execute(query, variable_values=dict(
                project_key=fixture.project.key,
                before=start_date + timedelta(days=30),
                days=30,
                window=30,
                sample=30
            ))
            assert not result.get('errors')
            project = result['data']['project']
            assert len(project['arrivalDepartureTrends']) == 2

            assert  [
                measurement['arrivalRate']
                for measurement in project['arrivalDepartureTrends']
            ] == [
                1,0
            ]

        def it_records_all_the_transitions_from_backlog_to_open_wip_and_complete_states(self, setup):
            fixture = setup
            api_helper = fixture.api_helper
            work_items = fixture.work_items
            start_date = fixture.start_date

            client = Client(schema)

            api_helper.import_work_items(work_items)

            # one arrival in the period start_date, start_date + 30 days from backlog to open
            api_helper.update_work_items([(0, 'upnext', start_date + timedelta(days=2))])

            #another arrival in the period start_date + 30 days, start_date + 60 days, from backlog to wip
            api_helper.update_work_items([(1, 'doing', start_date + timedelta(days=32))])

            # another arrival in the period start_date + 30 days, start_date + 60 days, from backlog to complete
            api_helper.update_work_items([(2, 'done', start_date + timedelta(days=32))])

            result = client.execute(fixture.query, variable_values=dict(
                project_key=fixture.project.key,
                days=30,
                window=30,
                sample=30
            ))
            assert not result.get('errors')
            project = result['data']['project']
            assert len(project['arrivalDepartureTrends']) == 2

            assert  [
                measurement['arrivalRate']
                for measurement in project['arrivalDepartureTrends']
            ] == [
                2,1
            ]

        def it_records_daily_arrivals(self, setup):
            fixture = setup
            api_helper = fixture.api_helper
            work_items = fixture.work_items
            start_date = fixture.start_date

            client = Client(schema)

            api_helper.import_work_items(work_items)

            for i in range(0,10):
                # one new transition in each day over a 10 day period start from the end of the period.
                api_helper.update_work_items([(i, 'upnext', start_date + timedelta(days=60-i))])

            # show measurements over 10 day window, daily
            result = client.execute(fixture.query, variable_values=dict(
                project_key=fixture.project.key,
                days=10,
                window=1,
                sample=1
            ))
            assert not result.get('errors')
            project = result['data']['project']
            assert len(project['arrivalDepartureTrends']) == 11
            # we expect 10 arrivals corresponding to the 10 work items
            assert  [
                measurement['arrivalRate']
                for measurement in project['arrivalDepartureTrends'][0:10]
            ] == [
                1
                for i in range(0,10)
            ]
            # and an extra 0 value to denote the arrival rate for the n+1th period.
            assert project['arrivalDepartureTrends'][10]['arrivalRate'] == 0

        def it_reports_the_number_of_distinct_delivery_cycles_not_work_items(self, setup):
            fixture = setup
            api_helper = fixture.api_helper
            work_items = fixture.work_items
            start_date = fixture.start_date

            client = Client(schema)

            api_helper.import_work_items(work_items)

            # one arrival in the period start_date, start_date + 30 days
            api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=32))])

            # this one gets closed
            api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=33))])

            # gets reopened again
            api_helper.update_work_items([(0, 'backlog', start_date + timedelta(days=34))])

            # and "arrives" again
            api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=35))])

            result = client.execute(fixture.query, variable_values=dict(
                project_key=fixture.project.key,
                days=30,
                window=30,
                sample=30
            ))
            assert not result.get('errors')
            project = result['data']['project']
            assert len(project['arrivalDepartureTrends']) == 2
            # we expect to report the same work item arriving twice in period in different delivery
            # cycles as two separate arrivals.
            assert [
                       measurement['arrivalRate']
                       for measurement in project['arrivalDepartureTrends']
                   ] == [
                       2, 0
                   ]

        def it_reports_the_delivery_cycles_that_transition_from_closed_to_wip_without_passing_through_backlog(self, setup):
            fixture = setup
            api_helper = fixture.api_helper
            work_items = fixture.work_items
            start_date = fixture.start_date

            client = Client(schema)

            api_helper.import_work_items(work_items)

            # one arrival in the period start_date, start_date + 30 days
            api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=32))])

            # this one gets closed
            api_helper.update_work_items([(0, 'closed', start_date + timedelta(days=33))])

            # and "arrives" again without passing through backlog
            api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=35))])

            result = client.execute(fixture.query, variable_values=dict(
                project_key=fixture.project.key,
                days=30,
                window=30,
                sample=30
            ))
            assert not result.get('errors')
            project = result['data']['project']
            assert len(project['arrivalDepartureTrends']) == 2
            # we expect to report the same work item arriving twice in period in different delivery
            # cycles as two separate arrivals.
            assert [
                       measurement['arrivalRate']
                       for measurement in project['arrivalDepartureTrends']
                   ] == [
                       2, 0
                   ]



        def it_respects_the_specs_only_flag(self, setup):
            fixture = setup
            api_helper = fixture.api_helper
            work_items = fixture.work_items
            start_date = fixture.start_date

            client = Client(schema)

            api_helper.import_work_items(work_items)

            # one arrival in the period start_date, start_date + 30 days
            api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])

            #another arrival in the period start_date + 30 days, start_date + 60 days
            api_helper.update_work_items([(1, 'doing', start_date + timedelta(days=32))])

            #make the second one is a spec
            api_helper.update_delivery_cycle(1, dict(commit_count=1))

            query = """
                    query getProjectArrivalDepartureTrends(
                        $project_key:String!,
                        $days: Int!,
                        $window: Int!,
                        $sample: Int
                    ) {
                        project(
                            key: $project_key,
                            interfaces: [ArrivalDepartureTrends],
                            arrivalDepartureTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
                                specsOnly: true
                            },
                            
                        )
                        {
                            name
                            key
                            arrivalDepartureTrends {
                                measurementDate
                                measurementWindow
                                arrivalRate
                            }
                        }
                    }
                    """

            result = client.execute(query, variable_values=dict(
                project_key=fixture.project.key,
                days=30,
                window=30,
                sample=30
            ))
            assert not result.get('errors')
            project = result['data']['project']
            assert len(project['arrivalDepartureTrends']) == 2

            assert  [
                measurement['arrivalRate']
                for measurement in project['arrivalDepartureTrends']
            ] == [
                1,0
            ]

        def it_excludes_subtasks_if_specified(self, setup):
            fixture = setup
            api_helper = fixture.api_helper
            work_items = fixture.work_items
            start_date = fixture.start_date

            client = Client(schema)

            api_helper.import_work_items(work_items)

            # one arrival in the period start_date, start_date + 30 days
            api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])

            #another arrival in the period start_date + 30 days, start_date + 60 days
            api_helper.update_work_items([(1, 'doing', start_date + timedelta(days=32))])

            #make the second one a subtask
            api_helper.update_work_item_attributes(1, dict(work_item_type='subtask'))

            query = """
                    query getProjectArrivalDepartureTrends(
                        $project_key:String!,
                        $days: Int!,
                        $window: Int!,
                        $sample: Int
                    ) {
                        project(
                            key: $project_key,
                            interfaces: [ArrivalDepartureTrends],
                            arrivalDepartureTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
                                includeSubTasks: false
                            }
                        )
                        {
                            name
                            key
                            arrivalDepartureTrends {
                                measurementDate
                                measurementWindow
                                arrivalRate
                            }
                        }
                    }
                    """


            result = client.execute(query, variable_values=dict(
                project_key=fixture.project.key,
                days=30,
                window=30,
                sample=30
            ))
            assert not result.get('errors')
            project = result['data']['project']
            assert len(project['arrivalDepartureTrends']) == 2

            assert  [
                measurement['arrivalRate']
                for measurement in project['arrivalDepartureTrends']
            ] == [
                0,1
            ]

        def it_respects_tags_if_specified(self, setup):
            fixture = setup
            api_helper = fixture.api_helper
            work_items = fixture.work_items
            start_date = fixture.start_date

            client = Client(schema)

            api_helper.import_work_items(work_items)

            # one arrival in the period start_date, start_date + 30 days
            api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])

            #another arrival in the period start_date + 30 days, start_date + 60 days
            api_helper.update_work_items([(1, 'doing', start_date + timedelta(days=32))])

            #add a tag to the second one
            api_helper.update_work_item_attributes(1, dict(tags=['feature']))

            query = """
                    query getProjectArrivalDepartureTrends(
                        $project_key:String!,
                        $days: Int!,
                        $window: Int!,
                        $sample: Int
                    ) {
                        project(
                            key: $project_key,
                            interfaces: [ArrivalDepartureTrends],
                            arrivalDepartureTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
                                tags: ["feature"]
                            }
                        )
                        {
                            name
                            key
                            arrivalDepartureTrends {
                                measurementDate
                                measurementWindow
                                arrivalRate
                            }
                        }
                    }
                    """


            result = client.execute(query, variable_values=dict(
                project_key=fixture.project.key,
                days=30,
                window=30,
                sample=30
            ))
            assert not result.get('errors')
            project = result['data']['project']
            assert len(project['arrivalDepartureTrends']) == 2

            assert  [
                measurement['arrivalRate']
                for measurement in project['arrivalDepartureTrends']
            ] == [
                1,0
            ]

        def it_respects_releases_if_specified(self, setup):
            fixture = setup
            api_helper = fixture.api_helper
            work_items = fixture.work_items
            start_date = fixture.start_date

            client = Client(schema)

            api_helper.import_work_items(work_items)

            # one arrival in the period start_date, start_date + 30 days
            api_helper.update_work_items([(0, 'doing', start_date + timedelta(days=2))])

            #another arrival in the period start_date + 30 days, start_date + 60 days
            api_helper.update_work_items([(1, 'doing', start_date + timedelta(days=32))])

            #add a tag to the second one
            api_helper.update_work_item_attributes(1, dict(releases=['v1.0']))

            query = """
                    query getProjectArrivalDepartureTrends(
                        $project_key:String!,
                        $days: Int!,
                        $window: Int!,
                        $sample: Int
                    ) {
                        project(
                            key: $project_key,
                            interfaces: [ArrivalDepartureTrends],
                            arrivalDepartureTrendsArgs: {
                                days: $days,
                                measurementWindow: $window,
                                samplingFrequency: $sample,
                                release: "v1.0"
                            }
                        )
                        {
                            name
                            key
                            arrivalDepartureTrends {
                                measurementDate
                                measurementWindow
                                arrivalRate
                            }
                        }
                    }
                    """


            result = client.execute(query, variable_values=dict(
                project_key=fixture.project.key,
                days=30,
                window=30,
                sample=30
            ))
            assert not result.get('errors')
            project = result['data']['project']
            assert len(project['arrivalDepartureTrends']) == 2

            assert  [
                measurement['arrivalRate']
                for measurement in project['arrivalDepartureTrends']
            ] == [
                1,0
            ]