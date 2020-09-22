# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import pytest
from datetime import datetime, timedelta
from graphene.test import Client
from polaris.analytics.service.graphql import schema
from test.fixtures.graphql import graphql_date

class TrendingWindowTestNumberOfMeasurements:

        class WhenThereAreNoWorkItems:

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
                assert len(project[fixture.output_attribute]) == 31

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
                assert len(project[fixture.output_attribute]) == 31

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
                assert len(project[fixture.output_attribute]) == 31

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
                assert len(project[fixture.output_attribute]) == 5

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
                assert len(project[fixture.output_attribute]) == 5

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
                assert len(project[fixture.output_attribute]) == 2


class TrendingWindowMeasurementDate:

    def it_returns_a_measurement_date_and_window_for_each_measurement(self, setup):
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
        measurements = project[fixture.output_attribute]
        for measurement in measurements:
            assert measurement['measurementDate']
            assert measurement['measurementWindow']


    def it_returns_trends_in_descending_order_of_measurement_dates(self, setup):
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
        measurements = project[fixture.output_attribute]
        for i in range(1, len(measurements)):
            assert graphql_date(measurements[i]['measurementDate']) < graphql_date(measurements[i-1]['measurementDate'])

