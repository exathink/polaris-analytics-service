from graphene.test import Client

from polaris.analytics.db.enums import WorkItemsStateType
from polaris.analytics.messaging.subscribers import AnalyticsTopicSubscriber

from polaris.analytics.messaging import RecalculateCycleTimesForWorkItemSource
from polaris.analytics.service.graphql import schema
from polaris.common import db
from polaris.messaging.test_utils import mock_channel, fake_send, mock_publisher
from polaris.messaging.topics import AnalyticsTopic


from test.fixtures.project_work_items_commits import *

from test.fixtures.project_work_items import *
from polaris.analytics.messaging import RecalculateCycleTimesForWorkItemSource
class TestUpdateDeliveryCyclesOnUpdateStateMaps:

    def it_updates_delivery_cycles_when_closed_state_mapping_changes(self, work_items_delivery_cycles_setup):
        # example case to test:
        # when there was a work item in state done (mapped to complete)
        # corresponding delivery cycle would have lead time and end_date as null
        # when done is mapped to closed in new mapping, the delivery cycle should have a lead time calculated and end_date set

        project = work_items_delivery_cycles_setup
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id

        message = fake_send(
            RecalculateCycleTimesForWorkItemSource(
                send=dict(
                    project_key=project_key,
                    work_items_source_key=work_items_source_key,
                    rebuild_delivery_cycles=
                )
            )
        )
        channel = mock_channel()
        publisher = mock_publisher()

        AnalyticsTopicSubscriber(channel, publisher=publisher).dispatch(channel, message)

        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycles\
                     where work_item_delivery_cycles.work_item_id='{work_item_id}' and lead_time is not null").scalar() == 1

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
        delivery_cycle_id = db.connection().execute(
            f"select delivery_cycle_id from analytics.work_item_delivery_cycles\
                     where work_item_delivery_cycles.work_item_id='{work_item_id}' and lead_time is not null").fetchall()[
            0][0]

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
        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycles\
                                     where work_item_delivery_cycles.work_item_id='{work_item_id}' \
                                     and lead_time is null and delivery_cycle_id!={delivery_cycle_id}").scalar() == 1


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
        end_seq_no, end_date, lead_time = db.connection().execute(
            f"select end_seq_no, end_date, lead_time from analytics.work_item_delivery_cycles\
                     where work_item_delivery_cycles.work_item_id='{work_item_id}' and lead_time is not null").fetchall()[
            0]
        # Tricking the setup to have multiple closed state transition by changing state mappings
        # The delivery cycle should now have end_seq_no, end_date, lead_time calculated at first closed state transition
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
        new_end_seq_no, new_end_date, new_lead_time = db.connection().execute(
            f"select end_seq_no, end_date, lead_time from analytics.work_item_delivery_cycles\
                             where work_item_delivery_cycles.work_item_id='{work_item_id}' and lead_time is not null").fetchall()[
            0]
        assert new_end_seq_no < end_seq_no
        assert new_end_date < end_date
        assert new_lead_time < lead_time

    def it_does_not_update_delivery_cycle_when_closed_state_mapping_is_unchanged(self,
                                                                                 work_items_delivery_cycles_setup):
        client = Client(schema)
        project = work_items_delivery_cycles_setup
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
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
        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycles\
             where work_item_delivery_cycles.work_item_id='{work_item_id}' and lead_time is not null").scalar() == 0

    def it_does_not_create_new_delivery_cycle_on_transition_from_closed_state_to_another_closed_state(self,
                                                                                                      work_items_delivery_cycles_setup):
        client = Client(schema)
        project = work_items_delivery_cycles_setup
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
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
        assert db.connection().execute("select count(*) from analytics.work_item_delivery_cycles").scalar() == 1

    def it_is_idempotent(self, work_items_delivery_cycles_setup):
        # call twice with same inputs. Query both times to find same results in delivery cycle table
        # just to ensure there are no attempts to create duplicate entries or updates on lead time or end_date
        client = Client(schema)
        project = work_items_delivery_cycles_setup
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
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
        assert db.connection().execute("select count(*) from analytics.work_item_delivery_cycles").scalar() == 1
        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycles\
                             where work_item_delivery_cycles.work_item_id='{work_item_id}' and lead_time is not null").scalar() == 1

        # Repeat
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
        assert db.connection().execute("select count(*) from analytics.work_item_delivery_cycles").scalar() == 1
        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycles\
                                     where work_item_delivery_cycles.work_item_id='{work_item_id}' and lead_time is not null").scalar() == 1

    def it_validates_current_delivery_cycle_is_reset_after_state_map_updates(self, work_items_delivery_cycles_setup):
        client = Client(schema)
        project = work_items_delivery_cycles_setup
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
        old_delivery_cycle_id = \
            db.connection().execute("select current_delivery_cycle_id from analytics.work_items").fetchall()[0][0]
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
        assert db.connection().execute(
            f"select count(*) from analytics.work_items where current_delivery_cycle_id is not Null").scalar() == 1
        new_delivery_cycle_id = db.connection().execute(
            f"select current_delivery_cycle_id from analytics.work_items where id='{work_item_id}'").fetchall()[0][0]
        assert new_delivery_cycle_id is not None
        assert new_delivery_cycle_id != old_delivery_cycle_id


class TestUpdateDeliveryCycleDurationsOnUpdateStateMaps:

    def it_recomputes_delivery_cycle_durations_when_closed_state_type_mapping_changes(self,
                                                                                      work_items_delivery_cycles_setup):
        client = Client(schema)
        project = work_items_delivery_cycles_setup
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
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
        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycles\
                             where work_item_delivery_cycles.work_item_id='{work_item_id}' and lead_time is not null").scalar() == 1
        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycle_durations\
                                     where cumulative_time_in_state is not null and state='created'").scalar() == 1


class TestRecomputeDeliveryCyclesCycleTimeOnUpdateStateMaps:

    def it_recomputes_delivery_cycle_cycle_time_when_state_type_mapping_changes(self,
                                                                                work_items_delivery_cycles_setup):
        client = Client(schema)
        project = work_items_delivery_cycles_setup
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id

        # check cycle time, should be null initially
        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycles\
                                     where work_item_delivery_cycles.work_item_id='{work_item_id}' and cycle_time is NULL").scalar() == 1
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
        # cycle time will be value greater than zero, as created state is mapped to open
        # note there is only 1 delivery cycle in this case
        _delivery_cycle_id, cycle_time = db.connection().execute(
            f"select delivery_cycle_id, cycle_time from analytics.work_item_delivery_cycles\
                             where work_item_delivery_cycles.work_item_id='{work_item_id}' and cycle_time > 0").fetchall()[
            0]
        expected_cycle_time = db.connection().execute(
            f"select sum(cumulative_time_in_state) from analytics.work_item_delivery_cycle_durations\
                                     where state in ('created', 'doing') and delivery_cycle_id={_delivery_cycle_id}").fetchall()[
            0][0]
        assert expected_cycle_time == cycle_time


class TestRecomputeWorkItemCommitDeliveryCycleMappingOnUpdateStateMaps:
    def it_updates_commit_stats_for_recreated_delivery_cycles(self, project_work_items_commits_fixture):
        client = Client(schema)
        project = project_work_items_commits_fixture
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
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
        assert db.connection().execute(
            "select count(*) from analytics.work_items_commits where delivery_cycle_id is null"
        ).scalar() == 0


class TestUpdateCommitStatsOnUpdateStateMaps:

    def it_updates_commit_stats_for_recreated_delivery_cycles(self, project_work_items_commits_fixture):
        client = Client(schema)
        project = project_work_items_commits_fixture
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
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
        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycles\
                                     where work_item_delivery_cycles.work_item_id='{work_item_id}' and repository_count=3 and commit_count=3 and latest_commit='2020-01-07 00:00:00.000000' and earliest_commit='2020-01-05 00:00:00.000000'").scalar() == 1

    def it_updates_implementation_effort_for_recreated_delivery_cycles(self, project_work_items_commits_fixture):
        client = Client(schema)
        project = project_work_items_commits_fixture
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
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
        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycles\
                                     where work_item_delivery_cycles.work_item_id='{work_item_id}' and effort=3"
        ).scalar() == 1

    def it_updates_commit_stats_for_recreated_delivery_cycles_for_multiple_closed_states(self,
                                                                                         project_work_items_commits_fixture):
        client = Client(schema)
        project = project_work_items_commits_fixture
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
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
        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycles\
                                             where work_item_delivery_cycles.work_item_id='{work_item_id}' and repository_count=3 and commit_count=3 and latest_commit='2020-01-07 00:00:00.000000' and earliest_commit='2020-01-05 00:00:00.000000'").scalar() == 1


class TestComputeImplementationComplexityMetricsOnUpdateStateMaps:

    def it_recomputes_complexity_metrics_for_recreated_delivery_cycles(self, project_work_items_commits_fixture):
        client = Client(schema)
        project = project_work_items_commits_fixture
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
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
        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycles where work_item_id={work_item_id} and total_lines_changed_merge=8 \
            and total_files_changed_merge=1 and average_lines_changed_merge=8 and total_lines_changed_non_merge=16 and \
            total_files_changed_non_merge=2 and total_lines_deleted_non_merge=8 \
            and total_lines_inserted_non_merge=8").scalar() == 1

    def it_recomputes_complexity_metrics_for_recreated_delivery_cycles_for_multiple_closed_states(self,
                                                                                                  project_work_items_commits_fixture):
        client = Client(schema)
        project = project_work_items_commits_fixture
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
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
        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycles where work_item_id={work_item_id} and total_lines_changed_merge=0 \
                    and total_files_changed_merge=0 and average_lines_changed_merge=0 and total_lines_changed_non_merge=8 and \
                    total_files_changed_non_merge=1 and total_lines_deleted_non_merge=4 \
                    and total_lines_inserted_non_merge=4").scalar() == 1


class TestComputeContributorMetricsOnUpdateStateMaps:

    def it_recomputes_contributor_metrics_for_recreated_delivery_cycles(self, project_work_items_commits_fixture):
        client = Client(schema)
        project = project_work_items_commits_fixture
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
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
        # single author/committer, 3 commits with 8 lines in each, 1 delivery cycle
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors \
            where total_lines_as_author=16 and total_lines_as_reviewer=8").scalar() == 1


class TestPopulateWorkItemSourceFileChangesOnUpdateStateMaps:

    def it_populates_work_item_source_file_changes_for_recreated_delivery_cycles(self,
                                                                                 project_work_items_commits_fixture):
        client = Client(schema)
        project = project_work_items_commits_fixture
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
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
        # 2 source files each, for 3 commit, 1 work item
        assert db.connection().execute(
            "select count(distinct (source_file_id, commit_id, work_item_id)) from analytics.work_item_source_file_changes where delivery_cycle_id is not NULL").scalar() == 6