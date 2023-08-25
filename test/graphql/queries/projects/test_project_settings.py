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
from polaris.analytics.service.graphql.interfaces import FlowMetricsSettingsImpl


class TestProjectSettings:
    class TestFlowMetricsSettings:
        @pytest.fixture
        def setup(self, setup_projects):
            project = test_projects[0]

            query = """
                    query getProjectSettings($project_key:String!) {
                        project(key: $project_key) {
                            settings {
                                flowMetricsSettings {
                                    cycleTimeTarget
                                    leadTimeTarget
                                    responseTimeConfidenceTarget
                                    leadTimeConfidenceTarget
                                    cycleTimeConfidenceTarget
                                    includeSubTasks
                                }
                            }
                        }
                    }
            """
            yield Fixture(
                project=project,
                query=query
            )

        class WhenSettingsIsNull:
            @pytest.fixture
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
                assert not flow_metrics['includeSubTasks']

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
                assert not flow_metrics['includeSubTasks']

        class WhenSettingsIsNotEmpty:
            @pytest.fixture
            def setup(self, setup):
                fixture = setup
                settings_fixture = Fixture(
                    flow_metrics_settings=Fixture(
                        cycle_time_target=7,
                        lead_time_target=14,
                        response_time_confidence_target=0.7,
                        lead_time_confidence_target=0.9,
                        cycle_time_confidence_target=0.75,
                        include_sub_tasks=True
                    )

                )
                with db.orm_session() as session:
                    project = Project.find_by_project_key(session, fixture.project['key'])
                    project.update_settings(
                        settings_fixture
                    )

                yield Fixture(
                    parent=fixture,
                    settings=settings_fixture
                )

            def it_shows_flow_metrics_settings(self, setup):
                fixture = setup

                client = Client(schema)
                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project['key']))
                assert 'data' in result
                project = result['data']['project']
                assert project['settings']
                flow_metrics_settings = project['settings']['flowMetricsSettings']
                assert flow_metrics_settings

                assert flow_metrics_settings['cycleTimeTarget'] \
                       == fixture.settings.flow_metrics_settings.cycle_time_target

                assert flow_metrics_settings['leadTimeTarget'] \
                       == fixture.settings.flow_metrics_settings.lead_time_target

                assert flow_metrics_settings['leadTimeConfidenceTarget'] \
                       == fixture.settings.flow_metrics_settings.lead_time_confidence_target

                assert flow_metrics_settings['cycleTimeConfidenceTarget'] \
                       == fixture.settings.flow_metrics_settings.cycle_time_confidence_target

                assert flow_metrics_settings['includeSubTasks'] \
                       == fixture.settings.flow_metrics_settings.include_sub_tasks

    class TestAnalysisPeriods:
        @pytest.fixture
        def setup(self, setup_projects):
            project = test_projects[0]

            query = """
                    query getProjectSettings($project_key:String!) {
                        project(key: $project_key) {
                            settings {
                                analysisPeriods {
                                    wipAnalysisPeriod
                                    flowAnalysisPeriod
                                    trendsAnalysisPeriod
                                }
                            }
                        }
                    }
            """
            yield Fixture(
                project=project,
                query=query
            )

        class WhenSettingsIsNull:
            @pytest.fixture
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
                flow_metrics = project['settings']['analysisPeriods']
                assert flow_metrics
                assert not flow_metrics['wipAnalysisPeriod']
                assert not flow_metrics['flowAnalysisPeriod']
                assert not flow_metrics['trendsAnalysisPeriod']

        class WhenSettingsIsEmpty:

            def it_returns_a_valid_result(self, setup):
                fixture = setup

                client = Client(schema)
                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project['key']))
                assert 'data' in result
                project = result['data']['project']
                assert project['settings']
                flow_metrics = project['settings']['analysisPeriods']
                assert flow_metrics
                assert not flow_metrics['wipAnalysisPeriod']
                assert not flow_metrics['flowAnalysisPeriod']
                assert not flow_metrics['trendsAnalysisPeriod']

        class WhenSettingsIsNotEmpty:
            @pytest.fixture
            def setup(self, setup):
                fixture = setup
                settings_fixture = Fixture(
                    analysis_periods=Fixture(
                        wip_analysis_period=7,
                        flow_analysis_period=14,
                        trends_analysis_period=30
                    )

                )
                with db.orm_session() as session:
                    project = Project.find_by_project_key(session, fixture.project['key'])
                    project.update_settings(
                        settings_fixture
                    )

                yield Fixture(
                    parent=fixture,
                    settings=settings_fixture
                )

            def it_shows_analysis_periods(self, setup):
                fixture = setup

                client = Client(schema)
                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project['key']))
                assert 'data' in result
                project = result['data']['project']
                assert project['settings']
                analysis_periods = project['settings']['analysisPeriods']
                assert analysis_periods

                assert analysis_periods['wipAnalysisPeriod'] \
                       == fixture.settings.analysis_periods.wip_analysis_period

                assert analysis_periods['flowAnalysisPeriod'] \
                       == fixture.settings.analysis_periods.flow_analysis_period

                assert analysis_periods['trendsAnalysisPeriod'] \
                       == fixture.settings.analysis_periods.trends_analysis_period

    class TestWipInspectorSettings:
        @pytest.fixture
        def setup(self, setup_projects):
            project = test_projects[0]

            query = """
                    query getProjectSettings($project_key:String!) {
                        project(key: $project_key) {
                            settings {
                                wipInspectorSettings {
                                    includeSubTasks
                                }
                            }
                        }
                    }
            """
            yield Fixture(
                project=project,
                query=query
            )

        class WhenSettingsIsNull:
            @pytest.fixture
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
                wip_inspector = project['settings']['wipInspectorSettings']
                assert wip_inspector
                assert wip_inspector['includeSubTasks'] is True

        class WhenSettingsIsEmpty:

            def it_returns_a_valid_result(self, setup):
                fixture = setup

                client = Client(schema)
                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project['key']))
                assert 'data' in result
                project = result['data']['project']
                assert project['settings']
                wip_inspector = project['settings']['wipInspectorSettings']
                assert wip_inspector
                assert wip_inspector['includeSubTasks'] is True

        class WhenSettingsIsNotEmpty:
            @pytest.fixture
            def setup(self, setup):
                fixture = setup
                settings_fixture = Fixture(
                    wip_inspector_settings=Fixture(
                        include_sub_tasks=True
                    )

                )
                with db.orm_session() as session:
                    project = Project.find_by_project_key(session, fixture.project['key'])
                    project.update_settings(
                        settings_fixture
                    )

                yield Fixture(
                    parent=fixture,
                    settings=settings_fixture
                )

            def it_shows_wip_inspector_settings(self, setup):
                fixture = setup

                client = Client(schema)
                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project['key']))
                assert 'data' in result
                project = result['data']['project']
                assert project['settings']
                wip_inspector_settings = project['settings']['wipInspectorSettings']
                assert wip_inspector_settings

                assert wip_inspector_settings['includeSubTasks'] \
                       == fixture.settings.wip_inspector_settings.include_sub_tasks
    
    class TestReleasesSettings:
        @pytest.fixture
        def setup(self, setup_projects):
            project = test_projects[0]

            query = """
                    query getProjectSettings($project_key:String!) {
                        project(key: $project_key) {
                            settings {
                                releasesSettings {
                                    enableReleases
                                }
                            }
                        }
                    }
            """
            yield Fixture(
                project=project,
                query=query
            )

        class WhenSettingsIsNull:
            @pytest.fixture
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
                releases = project['settings']['releasesSettings']
                assert releases
                assert releases['enableReleases'] is False


        class WhenSettingsIsEmpty:

            def it_returns_a_valid_result(self, setup):
                fixture = setup

                client = Client(schema)
                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project['key']))
                assert 'data' in result
                project = result['data']['project']
                assert project['settings']
                releases = project['settings']['releasesSettings']
                assert releases
                assert releases['enableReleases'] is False


        class WhenSettingsIsNotEmpty:
            @pytest.fixture
            def setup(self, setup):
                fixture = setup
                settings_fixture = Fixture(
                    releases_settings=Fixture(
                        enable_releases=True
                    )

                )
                with db.orm_session() as session:
                    project = Project.find_by_project_key(session, fixture.project['key'])
                    project.update_settings(
                        settings_fixture
                    )

                yield Fixture(
                    parent=fixture,
                    settings=settings_fixture
                )

            def it_shows_releases_settings(self, setup):
                fixture = setup

                client = Client(schema)
                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project['key']))
                assert 'data' in result
                project = result['data']['project']
                assert project['settings']
                releases_settings = project['settings']['releasesSettings']
                assert releases_settings

                assert releases_settings['enableReleases'] \
                       == fixture.settings.releases_settings.enable_releases

    class TestPhaseMappings:
        @pytest.fixture
        def setup(self, setup_projects):
            project = test_projects[0]

            query = """
                    query getProjectSettingsCustomPhaseMapping($project_key:String!) {
                        project(key: $project_key) {
                            settings {
                                customPhaseMapping {
                                    backlog
                                    open
                                    wip
                                    complete
                                    closed
                                }
                            }
                        }
                    }
            """
            yield Fixture(
                project=project,
                query=query
            )

        class WhenSettingsIsNull:
            @pytest.fixture
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
                custom_phase_mapping = project['settings']['customPhaseMapping']

                for phase in ['backlog', 'open', 'wip', 'complete', 'closed']:
                    assert custom_phase_mapping[phase] is None

        @pytest.mark.skip
        class WhenSettingsIsEmpty:

            def it_returns_a_valid_result(self, setup):
                fixture = setup

                client = Client(schema)
                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project['key']))
                assert 'data' in result
                project = result['data']['project']
                assert project['settings']
                releases = project['settings']['customPhaseMapping']
                assert releases
                assert releases['customPhaseMapping'] is None

        @pytest.mark.skip
        class WhenSettingsIsNotEmpty:
            @pytest.fixture
            def setup(self, setup):
                fixture = setup
                settings_fixture = Fixture(
                    custom_phase_mapping=Fixture(
                        backlog="Product Management",
                        open="Open",
                        wip="Engineering",
                        complete="QA/Release",
                        closed="Closed"
                    )

                )
                with db.orm_session() as session:
                    project = Project.find_by_project_key(session, fixture.project['key'])
                    project.update_settings(
                        settings_fixture
                    )

                yield Fixture(
                    parent=fixture,
                    settings=settings_fixture
                )

            def it_shows_custom_phase_mapping(self, setup):
                fixture = setup

                client = Client(schema)
                result = client.execute(fixture.query, variable_values=dict(project_key=fixture.project['key']))
                assert 'data' in result
                project = result['data']['project']
                assert project['settings']
                custom_phase_mapping = project['settings']['customPhaseMapping']
                assert custom_phase_mapping

                assert custom_phase_mapping == fixture.settings.custom_phase_mapping