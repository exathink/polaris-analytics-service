# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import pytest
from polaris.common import db
from polaris.analytics.db.model import Project

from graphene.test import Client
from polaris.analytics.service.graphql import schema
from datetime import datetime
from polaris.utils.collections import Fixture
from test.fixtures.project_work_items import *


class TestProjectSettings:
    class TestFlowMetricsSettings:
        @pytest.yield_fixture
        def setup(self, setup_projects):
            project = test_projects[0]
            settings_fixture = Fixture(
                flow_metrics_settings=Fixture(
                    cycle_time_target=7,
                    lead_time_target=14,
                    response_time_confidence_target=0.7
                )
            )
            query = """
                    query getProjectSettings($project_key:String!) {
                        project(key: $project_key) {
                            settings {
                                flowMetricsSettings {
                                    cycleTimeTarget
                                    leadTimeTarget
                                    responseTimeConfidenceTarget
                                }
                            }
                        }
                    }
            """
            yield Fixture(
                project=project,
                settings=settings_fixture,
                query=query
            )

        class WhenSettingsIsNull:
            @pytest.yield_fixture
            def setup(self, setup):
                fixture = setup

                with db.orm_session() as session:
                    project = Project.find_by_project_key(session, fixture.project['key'])
                    project.settings = None

                yield fixture

            def it_returns_a_valid_result(self, setup):
                fixture = setup

                client = Client(schema)
                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project['key']))
                assert 'data' in result
                project = result['data']['project']
                assert project['settings']
                flow_metrics = project['settings']['flowMetricsSettings']
                assert flow_metrics
                assert not flow_metrics['leadTimeTarget']
                assert not flow_metrics['cycleTimeTarget']
                assert not flow_metrics['responseTimeConfidenceTarget']

        class WhenSettingsIsEmpty:

            def it_returns_a_valid_result(self, setup):
                fixture = setup

                client = Client(schema)
                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project['key']))
                assert 'data' in result
                project = result['data']['project']
                assert project['settings']
                flow_metrics = project['settings']['flowMetricsSettings']
                assert flow_metrics
                assert not flow_metrics['leadTimeTarget']
                assert not flow_metrics['cycleTimeTarget']
                assert not flow_metrics['responseTimeConfidenceTarget']

        class WhenSettingsIsNotEmpty:
            @pytest.yield_fixture
            def setup(self, setup):
                fixture = setup

                with db.orm_session() as session:
                    project = Project.find_by_project_key(session, fixture.project['key'])
                    project.update_settings(
                        fixture.settings
                    )

                yield fixture

            def it_shows_flow_metrics_settings(self, setup):
                fixture = setup

                client = Client(schema)
                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project['key']))
                assert 'data' in result
                project = result['data']['project']
                assert project['settings']
                flow_metrics_settings = project['settings']['flowMetricsSettings']
                assert flow_metrics_settings

                assert flow_metrics_settings[
                           'cycleTimeTarget'] == fixture.settings.flow_metrics_settings.cycle_time_target
