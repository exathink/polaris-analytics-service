# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from graphene.test import Client
from unittest.mock import patch
from polaris.analytics.service.graphql import schema
from polaris.utils.collections import Fixture
from test.fixtures.project_work_items import *
from test.fixtures.project_work_items_commits import *

from polaris.analytics.db.enums import WorkItemsStateFlowType, WorkItemsStateReleaseStatusType


class TestArchiveProject:

    def it_archives_a_project(self, setup_projects):
        client = Client(schema)

        response = client.execute("""
            mutation archiveProject($archiveProjectInput: ArchiveProjectInput!) {
                archiveProject(archiveProjectInput:$archiveProjectInput) {
                    projectName
                }
            }
        """, variable_values=dict(
            archiveProjectInput=dict(
                projectKey=str(test_projects[0]['key'])
            )
        ))
        assert response['data']['archiveProject']['projectName'] == 'mercury'
        assert db.connection().execute(
            f"select archived from analytics.projects where key='{test_projects[0]['key']}'"
        ).scalar()


class TestUpdateProjectStateMaps:

    def it_updates_project_work_items_source_state_map(self, setup_work_items_sources):
        client = Client(schema)
        project = setup_work_items_sources
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        with patch('polaris.analytics.publish.publish'):
            response = client.execute("""
                mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                            success
                        }
                    }
            """, variable_values=dict(
                updateProjectStateMapsInput=dict(
                    projectKey=project_key,
                    workItemsSourceStateMaps=[
                        dict(
                            workItemsSourceKey=work_items_source_key,
                            stateMaps=[
                                dict(state="created", stateType=WorkItemsStateType.backlog.value),
                                dict(state="todo", stateType=WorkItemsStateType.open.value),
                                dict(state="doing", stateType=WorkItemsStateType.wip.value),
                                dict(state="done", stateType=WorkItemsStateType.complete.value),
                                dict(state="closed", stateType=WorkItemsStateType.closed.value)
                            ]
                        )
                    ]
                )
            )
                                      )
            assert 'data' in response
            result = response['data']['updateProjectStateMaps']
            assert result
            assert result['success']

    def it_updates_project_work_items_flow_types(self, setup_work_items_sources):
        client = Client(schema)
        project = setup_work_items_sources
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        with patch('polaris.analytics.publish.publish'):
            response = client.execute("""
                mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                            success
                        }
                    }
            """, variable_values=dict(
                updateProjectStateMapsInput=dict(
                    projectKey=project_key,
                    workItemsSourceStateMaps=[
                        dict(
                            workItemsSourceKey=work_items_source_key,
                            stateMaps=[
                                dict(state="created", stateType=WorkItemsStateType.backlog.value,
                                     flowType=WorkItemsStateFlowType.waiting.value),
                                dict(state="todo", stateType=WorkItemsStateType.open.value,
                                     flowType=WorkItemsStateFlowType.waiting.value),
                                dict(state="doing", stateType=WorkItemsStateType.wip.value,
                                     flowType=WorkItemsStateFlowType.active.value),
                                dict(state="done", stateType=WorkItemsStateType.complete.value,
                                     flowType=WorkItemsStateFlowType.waiting.value),
                                dict(state="closed", stateType=WorkItemsStateType.closed.value, flowType=None)
                            ]
                        )
                    ]
                )
            )
                                      )
            assert 'data' in response
            result = response['data']['updateProjectStateMaps']
            assert result
            assert result['success']
            saved_map = db.connection().execute(
                f"select state, flow_type from analytics.work_items_source_state_map "
                f"inner join analytics.work_items_sources on work_items_sources.id = work_items_source_state_map.work_items_source_id where key='{work_items_source_key}'"
            ).fetchall()
            assert {
                       (row.state, row.flow_type)
                       for row in saved_map
                   } == {
                       ('created', WorkItemsStateFlowType.waiting.value),
                       ('todo', WorkItemsStateFlowType.waiting.value),
                       ('doing', WorkItemsStateFlowType.active.value),
                       ('done', WorkItemsStateFlowType.waiting.value),
                       ('closed', None)
                   }

            assert saved_map

    def it_updates_project_work_items_release_status(self, setup_work_items_sources):
        client = Client(schema)
        project = setup_work_items_sources
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        with patch('polaris.analytics.publish.publish'):
            response = client.execute("""
                mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                            success
                        }
                    }
            """, variable_values=dict(
                updateProjectStateMapsInput=dict(
                    projectKey=project_key,
                    workItemsSourceStateMaps=[
                        dict(
                            workItemsSourceKey=work_items_source_key,
                            stateMaps=[
                                dict(state="created", stateType=WorkItemsStateType.backlog.value, releaseStatus=None),
                                dict(state="todo", stateType=WorkItemsStateType.open.value, releaseStatus=None),
                                dict(state="doing", stateType=WorkItemsStateType.wip.value, releaseStatus=None),
                                dict(state="done", stateType=WorkItemsStateType.complete.value,
                                     releaseStatus=WorkItemsStateReleaseStatusType.releasable.value),
                                dict(state="closed", stateType=WorkItemsStateType.closed.value,
                                     releaseStatus=WorkItemsStateReleaseStatusType.released.value)
                            ]
                        )
                    ]
                )
            )
                                      )
            assert 'data' in response
            result = response['data']['updateProjectStateMaps']
            assert result
            assert result['success']
            saved_map = db.connection().execute(
                f"select state, release_status from analytics.work_items_source_state_map "
                f"inner join analytics.work_items_sources on work_items_sources.id = work_items_source_state_map.work_items_source_id where key='{work_items_source_key}'"
            ).fetchall()
            assert {
                       (row.state, row.release_status)
                       for row in saved_map
                   } == {
                       ('created', None),
                       ('todo', None),
                       ('doing', None),
                       ('done', WorkItemsStateReleaseStatusType.releasable.value),
                       ('closed', WorkItemsStateReleaseStatusType.released.value)
                   }

    def it_checks_if_project_exists(self, setup_work_items_sources):
        client = Client(schema)
        project = setup_work_items_sources
        project_key = str(uuid.uuid4())
        work_items_source_key = project.work_items_sources[0].key
        response = client.execute("""
                    mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                    updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                                success
                                errorMessage
                            }
                        }
                """, variable_values=dict(
            updateProjectStateMapsInput=dict(
                projectKey=project_key,
                workItemsSourceStateMaps=[
                    dict(
                        workItemsSourceKey=work_items_source_key,
                        stateMaps=[
                            dict(state="todo", stateType=WorkItemsStateType.open.value),
                            dict(state="doing", stateType=WorkItemsStateType.wip.value),
                            dict(state="done", stateType=WorkItemsStateType.complete.value)
                        ]
                    )
                ]
            )
        )
                                  )
        assert 'data' in response
        result = response['data']['updateProjectStateMaps']
        assert result
        assert not result['success']
        assert result['errorMessage'] == f"Could not find project with key {project_key}"

    def it_checks_if_work_items_source_belongs_to_project(self, setup_work_items_sources):
        client = Client(schema)
        project = setup_work_items_sources
        project_key = str(project.key)
        work_items_source_key = str(uuid.uuid4())
        response = client.execute("""
                    mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                    updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                                success
                                errorMessage
                            }
                        }
                        """, variable_values=dict(
            updateProjectStateMapsInput=dict(
                projectKey=project_key,
                workItemsSourceStateMaps=[
                    dict(
                        workItemsSourceKey=work_items_source_key,
                        stateMaps=[
                            dict(state="todo", stateType=WorkItemsStateType.open.value),
                            dict(state="doing", stateType=WorkItemsStateType.wip.value),
                            dict(state="done", stateType=WorkItemsStateType.complete.value)
                        ]
                    )
                ]
            )
        )
                                  )
        assert 'data' in response
        result = response['data']['updateProjectStateMaps']
        assert not result['success']
        assert result[
                   'errorMessage'] == f"Work Items Source with key {work_items_source_key} does not belong to project"

    def it_checks_if_work_items_source_states_have_duplicates(self, setup_work_items_sources):
        client = Client(schema)
        project = setup_work_items_sources
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        response = client.execute("""
                    mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                    updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                                success
                                errorMessage
                            }
                        }
                        """, variable_values=dict(
            updateProjectStateMapsInput=dict(
                projectKey=project_key,
                workItemsSourceStateMaps=[
                    dict(
                        workItemsSourceKey=work_items_source_key,
                        stateMaps=[
                            dict(state="todo", stateType=WorkItemsStateType.open.value),
                            dict(state="todo", stateType=WorkItemsStateType.wip.value),
                            dict(state="done", stateType=WorkItemsStateType.complete.value)
                        ]
                    )
                ]
            )
        )
                                  )
        assert 'data' in response
        result = response['data']['updateProjectStateMaps']
        assert not result['success']
        assert result['errorMessage'] == "Invalid state map: duplicate states in the input"

    def it_validates_that_state_types_have_legal_values(self, setup_work_items_sources):
        client = Client(schema)
        project = setup_work_items_sources
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        response = client.execute("""
                            mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                            updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                                        success
                                        errorMessage
                                    }
                                }
                        """, variable_values=dict(
            updateProjectStateMapsInput=dict(
                projectKey=project_key,
                workItemsSourceStateMaps=[
                    dict(
                        workItemsSourceKey=work_items_source_key,
                        stateMaps=[
                            dict(state="todo", stateType="opened"),

                        ]
                    )
                ]
            )
        )
                                  )
        assert 'errors' in response
        message = response['errors'][0]['message']
        assert 'got invalid value' in message

    def it_validates_duplicate_closed_state(self, setup_work_items_sources):
        client = Client(schema)
        project = setup_work_items_sources
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        with patch('polaris.analytics.publish.publish'):
            response = client.execute("""
                mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                            success
                        }
                    }
            """, variable_values=dict(
                updateProjectStateMapsInput=dict(
                    projectKey=project_key,
                    workItemsSourceStateMaps=[
                        dict(
                            workItemsSourceKey=work_items_source_key,
                            stateMaps=[
                                dict(state="created", stateType=WorkItemsStateType.backlog.value),
                                dict(state="todo", stateType=WorkItemsStateType.open.value),
                                dict(state="doing", stateType=WorkItemsStateType.wip.value),
                                dict(state="done", stateType=WorkItemsStateType.closed.value),
                                dict(state="closed", stateType=WorkItemsStateType.closed.value)
                            ]
                        )
                    ]
                )
            )
                                      )
            assert 'data' in response
            result = response['data']['updateProjectStateMaps']
            assert result
            assert result['success']


class TestUpdateComputedWorkItemsStateTypes:

    def it_updates_computed_state_types_when_a_state_map_is_initialized(self, setup_work_items):
        client = Client(schema)
        project = setup_work_items
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_key = project.work_items_sources[0].work_items[0].key
        with patch('polaris.analytics.publish.publish'):
            response = client.execute("""
                mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                            success
                        }
                    }
            """, variable_values=dict(
                updateProjectStateMapsInput=dict(
                    projectKey=project_key,
                    workItemsSourceStateMaps=[
                        dict(
                            workItemsSourceKey=work_items_source_key,
                            stateMaps=[
                                dict(state="todo", stateType=WorkItemsStateType.open.value),
                                dict(state="doing", stateType=WorkItemsStateType.wip.value),
                                dict(state="done", stateType=WorkItemsStateType.complete.value),
                                dict(state="accepted", stateType=WorkItemsStateType.closed.value)
                            ]
                        )
                    ]
                )
            )
                                      )
            assert 'data' in response
            result = response['data']['updateProjectStateMaps']
            assert result
            assert result['success']

            work_items_result = db.connection().execute(f"select state, state_type from analytics.work_items "
                                                        f"inner join analytics.work_items_sources "
                                                        f"on work_items.work_items_source_id = work_items_sources.id "
                                                        f"where work_items_sources.key = '{work_items_source_key}'").fetchall()

            assert len(work_items_result) == 3
            assert set([
                (work_item.state, work_item.state_type)
                for work_item in work_items_result
            ]) == {
                       ('todo', WorkItemsStateType.open.value),
                       ('doing', WorkItemsStateType.wip.value),
                       ('done', WorkItemsStateType.complete.value)
                   }

    def it_updates_computed_state_types_after_initial_update(self, setup_work_items):
        client = Client(schema)
        project = setup_work_items
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_key = project.work_items_sources[0].work_items[0].key
        with patch('polaris.analytics.publish.publish'):
            response = client.execute("""
                mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                            success
                        }
                    }
            """, variable_values=dict(
                updateProjectStateMapsInput=dict(
                    projectKey=project_key,
                    workItemsSourceStateMaps=[
                        dict(
                            workItemsSourceKey=work_items_source_key,
                            stateMaps=[
                                # making a nonsensical assignment here to make sure the update takes
                                dict(state="todo", stateType=WorkItemsStateType.wip.value),
                                dict(state="doing", stateType=WorkItemsStateType.complete.value),
                                dict(state="done", stateType=WorkItemsStateType.open.value)
                            ]
                        )
                    ]
                )
            )
                                      )
            assert 'data' in response
            result = response['data']['updateProjectStateMaps']
            assert result
            assert result['success']

            work_items_result = db.connection().execute(f"select state, state_type from analytics.work_items "
                                                        f"inner join analytics.work_items_sources "
                                                        f"on work_items.work_items_source_id = work_items_sources.id "
                                                        f"where work_items_sources.key = '{work_items_source_key}'").fetchall()

            assert len(work_items_result) == 3
            assert set([
                (work_item.state, work_item.state_type)
                for work_item in work_items_result
            ]) == {
                       ('todo', WorkItemsStateType.wip.value),
                       ('doing', WorkItemsStateType.complete.value),
                       ('done', WorkItemsStateType.open.value)
                   }

    def it_resets_unmapped_entries_to_null(self, setup_work_items):
        client = Client(schema)
        project = setup_work_items
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_key = project.work_items_sources[0].work_items[0].key
        with patch('polaris.analytics.publish.publish'):
            response = client.execute("""
                mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                            success
                        }
                    }
            """, variable_values=dict(
                updateProjectStateMapsInput=dict(
                    projectKey=project_key,
                    workItemsSourceStateMaps=[
                        dict(
                            workItemsSourceKey=work_items_source_key,
                            stateMaps=[
                                # Leaving out the doing state from the mapping
                                dict(state="todo", stateType=WorkItemsStateType.open.value),
                                dict(state="done", stateType=WorkItemsStateType.complete.value)
                            ]
                        )
                    ]
                )
            )
                                      )
            assert 'data' in response
            result = response['data']['updateProjectStateMaps']
            assert result
            assert result['success']

            work_items_result = db.connection().execute(f"select state, state_type from analytics.work_items "
                                                        f"inner join analytics.work_items_sources "
                                                        f"on work_items.work_items_source_id = work_items_sources.id "
                                                        f"where work_items_sources.key = '{work_items_source_key}'").fetchall()

            assert len(work_items_result) == 3
            assert set([
                (work_item.state, work_item.state_type)
                for work_item in work_items_result
            ]) == {
                       ('todo', WorkItemsStateType.open.value),
                       ('doing', None),
                       ('done', WorkItemsStateType.complete.value)
                   }


class TestUpdateDeliveryCyclesOnUpdateStateMaps:

    def it_updates_delivery_cycles_when_closed_state_mapping_changes(self, work_items_delivery_cycles_setup):
        # example case to test:
        # when there was a work item in state done (mapped to complete)
        # corresponding delivery cycle would have lead time and end_date as null
        # when done is mapped to closed in new mapping, the delivery cycle should have a lead time calculated and end_date set
        client = Client(schema)
        project = work_items_delivery_cycles_setup
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
        with patch('polaris.analytics.publish.publish'):
            response = client.execute("""
                        mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                        updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                                    success
                                    deliveryCyclesRebuilt
                                }
                            }
                    """, variable_values=dict(
                updateProjectStateMapsInput=dict(
                    projectKey=project_key,
                    workItemsSourceStateMaps=[
                        dict(
                            workItemsSourceKey=work_items_source_key,
                            stateMaps=[
                                dict(state="created", stateType=WorkItemsStateType.open.value),
                                dict(state="doing", stateType=WorkItemsStateType.wip.value),
                                dict(state="done", stateType=WorkItemsStateType.closed.value),
                            ]
                        )
                    ]
                )
            )
                                      )
        assert 'data' in response
        result = response['data']['updateProjectStateMaps']
        assert result
        assert result['success']
        assert result.get('deliveryCyclesRebuilt') == 1

    def it_updates_delivery_cycles_when_an_existing_non_mapped_state_mapping_changes(self,
                                                                                     work_items_delivery_cycles_setup):
        # example case to test:
        # when there was a work item in state done (mapped to complete)
        # corresponding delivery cycle would have lead time and end_date as null
        # when done is mapped to closed in new mapping, the delivery cycle should have a lead time calculated and end_date set
        client = Client(schema)
        project = work_items_delivery_cycles_setup
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
        # In first step changing only closed state mapping
        with patch('polaris.analytics.publish.publish'):
            response = client.execute("""
                        mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                        updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                                    success
                                    deliveryCyclesRebuilt
                                }
                            }
                    """, variable_values=dict(
                updateProjectStateMapsInput=dict(
                    projectKey=project_key,
                    workItemsSourceStateMaps=[
                        dict(
                            workItemsSourceKey=work_items_source_key,
                            stateMaps=[
                                dict(state="created", stateType=WorkItemsStateType.open.value),
                                dict(state="doing", stateType=WorkItemsStateType.wip.value),
                                dict(state="done", stateType=WorkItemsStateType.closed.value)
                            ]
                        )
                    ]
                )
            )
                                      )
            assert 'data' in response
            result = response['data']['updateProjectStateMaps']
            assert result
            assert result['success']
            assert result.get('deliveryCyclesRebuilt') == 1

        # add an unmapped state to the db
        with db.orm_session() as session:
            session.add(project)
            project.work_items_sources[0].work_items[0].state_transitions.extend([
                WorkItemStateTransition(
                    seq_no=3,
                    created_at=get_date("2020-03-21"),
                    state='selected for development',
                    previous_state='done'
                ),
            ])
        with patch('polaris.analytics.publish.publish'):
            response = client.execute("""
                                mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                                updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                                            success
                                            deliveryCyclesRebuilt
                                        }
                                    }
                            """, variable_values=dict(
                updateProjectStateMapsInput=dict(
                    projectKey=project_key,
                    workItemsSourceStateMaps=[
                        dict(
                            workItemsSourceKey=work_items_source_key,
                            stateMaps=[
                                dict(state="created", stateType=WorkItemsStateType.open.value),
                                dict(state="doing", stateType=WorkItemsStateType.wip.value),
                                dict(state="done", stateType=WorkItemsStateType.closed.value),
                                dict(state="selected for development", stateType=WorkItemsStateType.open.value)
                            ]
                        )
                    ]
                )
            )
                                      )
            assert 'data' in response
            result = response['data']['updateProjectStateMaps']
            assert result
            assert result['success']
            # new delivery cycle created
            assert result.get('deliveryCyclesRebuilt') == 1

    def it_updates_delivery_cycles_at_first_closed_state_transition_when_mapping_changes(self,
                                                                                         work_items_delivery_cycles_setup):
        # example case to test:
        # when there was a work item in state done (mapped to complete)
        # corresponding delivery cycle would have lead time and end_date as null
        # when done is mapped to closed in new mapping, the delivery cycle should have a lead time calculated and end_date set
        client = Client(schema)
        project = work_items_delivery_cycles_setup
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
        with patch('polaris.analytics.publish.publish'):
            response = client.execute("""
                        mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                        updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                                    success
                                    deliveryCyclesRebuilt
                                }
                            }
                    """, variable_values=dict(
                updateProjectStateMapsInput=dict(
                    projectKey=project_key,
                    workItemsSourceStateMaps=[
                        dict(
                            workItemsSourceKey=work_items_source_key,
                            stateMaps=[
                                dict(state="created", stateType=WorkItemsStateType.open.value),
                                dict(state="doing", stateType=WorkItemsStateType.wip.value),
                                dict(state="done", stateType=WorkItemsStateType.closed.value),
                            ]
                        )
                    ]
                )
            )
                                      )
            assert 'data' in response
            result = response['data']['updateProjectStateMaps']
            assert result
            assert result['success']
            assert result.get('deliveryCyclesRebuilt') == 1
        # Tricking the setup to have multiple closed state transition by changing state mappings
        # The delivery cycle should now have end_seq_no, end_date, lead_time calculated at first closed state transition
        with patch('polaris.analytics.publish.publish'):
            response = client.execute("""
                                mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                                updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                                            success
                                            deliveryCyclesRebuilt
                                        }
                                    }
                            """, variable_values=dict(
                updateProjectStateMapsInput=dict(
                    projectKey=project_key,
                    workItemsSourceStateMaps=[
                        dict(
                            workItemsSourceKey=work_items_source_key,
                            stateMaps=[
                                dict(state="created", stateType=WorkItemsStateType.open.value),
                                dict(state="doing", stateType=WorkItemsStateType.closed.value),
                                dict(state="done", stateType=WorkItemsStateType.closed.value),
                            ]
                        )
                    ]
                )
            )
                                      )
            assert 'data' in response
            result = response['data']['updateProjectStateMaps']
            assert result
            assert result['success']
            assert result.get('deliveryCyclesRebuilt') == 1

    def it_does_not_update_delivery_cycle_when_closed_state_mapping_is_unchanged(self,
                                                                                 work_items_delivery_cycles_setup):
        client = Client(schema)
        project = work_items_delivery_cycles_setup
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
        with patch('polaris.analytics.publish.publish'):
            response = client.execute("""
                                mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                                updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                                            success
                                            deliveryCyclesRebuilt
                                        }
                                    }
                            """, variable_values=dict(
                updateProjectStateMapsInput=dict(
                    projectKey=project_key,
                    workItemsSourceStateMaps=[
                        dict(
                            workItemsSourceKey=work_items_source_key,
                            stateMaps=[
                                dict(state="created", stateType=WorkItemsStateType.open.value),
                                dict(state="doing", stateType=WorkItemsStateType.wip.value),
                                dict(state="done", stateType=WorkItemsStateType.complete.value),
                                dict(state="accepted", stateType=WorkItemsStateType.closed.value)
                            ]
                        )
                    ]
                )
            )
                                      )
            assert 'data' in response
            result = response['data']['updateProjectStateMaps']
            assert result
            assert result['success']
            assert not result.get('deliveryCyclesRebuilt') == 0

    def it_does_not_create_new_delivery_cycle_on_transition_from_closed_state_to_another_closed_state(self,
                                                                                                      work_items_delivery_cycles_setup):
        client = Client(schema)
        project = work_items_delivery_cycles_setup
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
        with patch('polaris.analytics.publish.publish'):
            response = client.execute("""
                                        mutation updateProjectStateMaps($updateProjectStateMapsInput: UpdateProjectStateMapsInput!) {
                                                        updateProjectStateMaps(updateProjectStateMapsInput:$updateProjectStateMapsInput) {
                                                    success
                                                    deliveryCyclesRebuilt
                                                }
                                            }
                                    """, variable_values=dict(
                updateProjectStateMapsInput=dict(
                    projectKey=project_key,
                    workItemsSourceStateMaps=[
                        dict(
                            workItemsSourceKey=work_items_source_key,
                            stateMaps=[
                                dict(state="created", stateType=WorkItemsStateType.open.value),
                                dict(state="doing", stateType=WorkItemsStateType.closed.value),
                                dict(state="done", stateType=WorkItemsStateType.closed.value)
                            ]
                        )
                    ]
                )
            )
                                      )
            assert 'data' in response
            result = response['data']['updateProjectStateMaps']
            assert result
            assert result['success']
            assert not result.get('deliveryCyclesRebuilt') == 0


class TestUpdateProjectSettings:

    @pytest.fixture
    def setup(self, setup_projects):
        query = """
            mutation updatProjectSettings($updateProjectSettingsInput: UpdateProjectSettingsInput!) {
                updateProjectSettings(updateProjectSettingsInput:$updateProjectSettingsInput) {
                    success
                    errorMessage
                }
            }
        """
        yield Fixture(
            query=query
        )

    def it_updates_the_project_name(self, setup):
        fixture = setup

        client = Client(schema)
        response = client.execute(fixture.query, variable_values=dict(
            updateProjectSettingsInput=dict(
                key=str(test_projects[0]['key']),
                name='New name'
            )
        ))

        assert response['data']['updateProjectSettings']['success']
        with db.orm_session() as session:
            project = Project.find_by_project_key(session, test_projects[0]['key'])
            assert project.name == 'New name'

    def it_updates_the_flow_metrics_settings(self, setup):
        fixture = setup

        client = Client(schema)

        response = client.execute(fixture.query, variable_values=dict(
            updateProjectSettingsInput=dict(
                key=str(test_projects[0]['key']),
                flowMetricsSettings=dict(
                    cycleTimeTarget=7,
                    leadTimeTarget=14,
                    responseTimeConfidenceTarget=0.7,
                    leadTimeConfidenceTarget=0.8,
                    cycleTimeConfidenceTarget=0.9,
                    includeSubTasks=False
                )
            )
        ))
        assert response['data']['updateProjectSettings']['success']
        with db.orm_session() as session:
            project = Project.find_by_project_key(session, test_projects[0]['key'])
            assert project.settings['flow_metrics_settings'] == dict(
                cycle_time_target=7,
                lead_time_target=14,
                response_time_confidence_target=0.7,
                lead_time_confidence_target=0.8,
                cycle_time_confidence_target=0.9,
                include_sub_tasks=False
            )

    def it_modifies_only_the_flow_metrics_settings_that_were_passed(self, setup):
        fixture = setup

        client = Client(schema)

        # update once
        client.execute(fixture.query, variable_values=dict(
            updateProjectSettingsInput=dict(
                key=str(test_projects[0]['key']),
                flowMetricsSettings=dict(
                    cycleTimeTarget=7,
                    leadTimeTarget=14,
                    responseTimeConfidenceTarget=0.7,
                    leadTimeConfidenceTarget=0.8,
                    cycleTimeConfidenceTarget=0.9,
                    includeSubTasks=False
                )
            )
        ))

        # update again
        response = client.execute(fixture.query, variable_values=dict(
            updateProjectSettingsInput=dict(
                key=str(test_projects[0]['key']),
                flowMetricsSettings=dict(
                    cycleTimeTarget=8,
                    includeSubTasks=False
                )
            )
        ))
        assert response['data']['updateProjectSettings']['success']
        with db.orm_session() as session:
            project = Project.find_by_project_key(session, test_projects[0]['key'])
            assert project.settings['flow_metrics_settings'] == dict(
                cycle_time_target=8,
                lead_time_target=14,
                response_time_confidence_target=0.7,
                lead_time_confidence_target=0.8,
                cycle_time_confidence_target=0.9,
                include_sub_tasks=False
            )

    def it_updates_the_analysis_periods(self, setup):
        fixture = setup

        client = Client(schema)

        response = client.execute(fixture.query, variable_values=dict(
            updateProjectSettingsInput=dict(
                key=str(test_projects[0]['key']),
                analysisPeriods=dict(
                    wipAnalysisPeriod=14,
                    flowAnalysisPeriod=30,
                    trendsAnalysisPeriod=45
                )
            )
        ))
        assert response['data']['updateProjectSettings']['success']
        with db.orm_session() as session:
            project = Project.find_by_project_key(session, test_projects[0]['key'])
            assert project.settings['analysis_periods'] == dict(
                wip_analysis_period=14,
                flow_analysis_period=30,
                trends_analysis_period=45
            )

    def it_only_updates_the_analysis_periods_that_were_passed(self, setup):
        fixture = setup

        client = Client(schema)

        # update once
        response = client.execute(fixture.query, variable_values=dict(
            updateProjectSettingsInput=dict(
                key=str(test_projects[0]['key']),
                analysisPeriods=dict(
                    wipAnalysisPeriod=14,
                    flowAnalysisPeriod=30,
                    trendsAnalysisPeriod=45
                )
            )
        ))

        # update again
        response = client.execute(fixture.query, variable_values=dict(
            updateProjectSettingsInput=dict(
                key=str(test_projects[0]['key']),
                analysisPeriods=dict(
                    wipAnalysisPeriod=7,

                )
            )
        ))

        assert response['data']['updateProjectSettings']['success']
        with db.orm_session() as session:
            project = Project.find_by_project_key(session, test_projects[0]['key'])
            assert project.settings['analysis_periods'] == dict(
                wip_analysis_period=7,
                flow_analysis_period=30,
                trends_analysis_period=45
            )

    def it_updates_the_wip_inspector_settings(self, setup):
        fixture = setup

        client = Client(schema)

        response = client.execute(fixture.query, variable_values=dict(
            updateProjectSettingsInput=dict(
                key=str(test_projects[0]['key']),
                wipInspectorSettings=dict(
                    includeSubTasks=False
                )
            )
        ))
        assert response['data']['updateProjectSettings']['success']
        with db.orm_session() as session:
            project = Project.find_by_project_key(session, test_projects[0]['key'])
            assert project.settings['wip_inspector_settings'] == dict(
                include_sub_tasks=False
            )

    def it_modifies_only_the_wip_inpector_settings_that_were_passed(self, setup):
        fixture = setup

        client = Client(schema)

        # update once
        client.execute(fixture.query, variable_values=dict(
            updateProjectSettingsInput=dict(
                key=str(test_projects[0]['key']),
                wipInspectorSettings=dict(
                    includeSubTasks=True
                )
            )
        ))

        # update again
        response = client.execute(fixture.query, variable_values=dict(
            updateProjectSettingsInput=dict(
                key=str(test_projects[0]['key']),
                wipInspectorSettings=dict(
                    includeSubTasks=False

                )
            )
        ))
        assert response['data']['updateProjectSettings']['success']
        with db.orm_session() as session:
            project = Project.find_by_project_key(session, test_projects[0]['key'])
            assert project.settings['wip_inspector_settings'] == dict(
                include_sub_tasks=False
            )


class TestUpdateWorkItems:

    @pytest.fixture
    def setup(self, setup_work_items):
        project = setup_work_items
        work_items = project.work_items_sources[0].work_items
        query = """
                mutation updateProjectWorkItems($updateProjectWorkItemsInput: UpdateProjectWorkItemsInput!) {
                    updateProjectWorkItems(updateProjectWorkItemsInput:$updateProjectWorkItemsInput) {
                        updateStatus {
                            workItemsKeys
                            success
                            message
                            exception
                        }
                    }
                }
            """
        yield Fixture(
            project=project,
            work_items=work_items,
            query=query
        )

    def it_updates_budget_for_work_items(self, setup):
        fixture = setup

        client = Client(schema)

        response = client.execute(fixture.query, variable_values=dict(
            updateProjectWorkItemsInput=dict(
                projectKey=str(test_projects[0]['key']),
                workItemsInfo=[
                    dict(
                        workItemKey=fixture.work_items[0].key,
                        budget=2.5
                    ),
                    dict(
                        workItemKey=fixture.work_items[1].key,
                        budget=2
                    )
                ]
            )
        ))

        assert response['data']['updateProjectWorkItems']['updateStatus']['success']
        with db.orm_session() as session:
            work_item_1 = WorkItem.find_by_work_item_key(session, fixture.work_items[0].key)
            assert work_item_1.budget == 2.5
            work_item_2 = WorkItem.find_by_work_item_key(session, fixture.work_items[1].key)
            assert work_item_2.budget == 2

    def it_returns_exception_when_project_key_is_incorrect(self, setup):
        fixture = setup

        client = Client(schema)
        new_test_project_key = uuid.uuid4()

        response = client.execute(fixture.query, variable_values=dict(
            updateProjectWorkItemsInput=dict(
                projectKey=str(new_test_project_key),
                workItemsInfo=[
                    dict(
                        workItemKey=fixture.work_items[0].key,
                        budget=2.5
                    ),
                    dict(
                        workItemKey=fixture.work_items[1].key,
                        budget=2
                    )
                ]
            )
        ))

        assert not response['data']['updateProjectWorkItems']['updateStatus']['success']
        assert response['data']['updateProjectWorkItems']['updateStatus'][
                   'exception'] == f"Could not find project with key {new_test_project_key}"

    def it_returns_failure_when_work_item_key_is_incorrect(self, setup):
        fixture = setup

        client = Client(schema)
        new_test_work_item_key = uuid.uuid4()

        response = client.execute(fixture.query, variable_values=dict(
            updateProjectWorkItemsInput=dict(
                projectKey=str(test_projects[0]['key']),
                workItemsInfo=[
                    dict(
                        workItemKey=fixture.work_items[0].key,
                        budget=2.5
                    ),
                    dict(
                        workItemKey=new_test_work_item_key,
                        budget=2
                    )
                ]
            )
        ))

        assert not response['data']['updateProjectWorkItems']['updateStatus']['success']
        assert response['data']['updateProjectWorkItems']['updateStatus'][
                   'exception'] == "Could not update project work items"


class TestUpdateCustomTypeMapping:

    @pytest.fixture
    def setup(self, setup_work_items):
        project = setup_work_items
        yield Fixture(
            project=project,
            work_items_source=project.work_items_sources[0]
        )

    def it_updates_custom_type_mapping(self, setup):
        fixture = setup

        client = Client(schema)
        response = client.execute("""
            mutation updateProjectCustomTypeMappings($updateProjectCustomTypeMappingsInput: UpdateProjectCustomTypeMappingsInput!) {
                updateProjectCustomTypeMappings(updateProjectCustomTypeMappingsInput: $updateProjectCustomTypeMappingsInput) {
                    success
                    errorMessage
                }
            }
        """, variable_values=dict(
            updateProjectCustomTypeMappingsInput=dict(
                projectKey=str(fixture.project.key),
                workItemsSourceKeys=[str(fixture.work_items_source.key)],
                customTypeMappings=[
                    dict(
                        labels=["Epic"],
                        workItemType="epic"
                    ),
                    dict(
                        labels=["Story"],
                        workItemType="story"
                    ),
                    dict(
                        labels=["Task"],
                        workItemType="task"
                    ),
                    dict(
                        labels=["Bug"],
                        workItemType="bug"
                    ),
                ]
            )
        ))

        assert not response.get('errors')
        with db.orm_session() as session:
            work_items_source = WorkItemsSource.find_by_work_items_source_key(session, fixture.work_items_source.key)
            assert work_items_source.custom_type_mappings == [
                dict(
                    labels=["Epic"],
                    work_item_type="epic"
                ),
                dict(
                    labels=["Story"],
                    work_item_type="story"
                ),
                dict(
                    labels=["Task"],
                    work_item_type="task"
                ),
                dict(
                    labels=["Bug"],
                    work_item_type="bug"
                ),
            ]
