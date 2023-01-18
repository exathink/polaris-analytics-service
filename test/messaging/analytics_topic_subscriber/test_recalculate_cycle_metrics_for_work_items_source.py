import pytest
from graphene.test import Client
from unittest.mock import patch

from polaris.analytics.db.enums import WorkItemsStateType
from polaris.analytics.messaging.subscribers import AnalyticsTopicSubscriber

from polaris.analytics.messaging import RecalculateCycleMetricsForWorkItemSource
from polaris.analytics.service.graphql import schema
from polaris.common import db
from polaris.messaging.test_utils import mock_channel, fake_send, mock_publisher
from polaris.messaging.topics import AnalyticsTopic

from test.fixtures.project_work_items import *
from test.fixtures.project_work_items_commits import *

from polaris.analytics.messaging import RecalculateCycleMetricsForWorkItemSource

'''
This is a utility function to dispatch the main message processing workflow being tested in this module. 
'''
def fake_dispatch_message(project_key, work_items_source_key):
    message = fake_send(
        RecalculateCycleMetricsForWorkItemSource(
            send=dict(
                project_key=project_key,
                work_items_source_key=work_items_source_key,
                rebuild_delivery_cycles=True
            )
        )
    )
    channel = mock_channel()
    publisher = mock_publisher()
    AnalyticsTopicSubscriber(channel, publisher=publisher).dispatch(channel, message)


'''
Note that in this test suite we are relying on a mutation call to UpdateStateMaps to set up some 
tests correctly. This might seem odd as we are coupling two different subsystems in one test, but this
is the principal path by which the message processing this code processes is triggered. So in a
way  this is more reflective of the way this message listener will enocounter scenarios in real life, 
so we are using the mutation as setup here. This way if the mutation behavior changes in a breaking manner, we can expect
the test cases here to break as well. 
'''
def update_project_state_maps(project_key, work_items_source_key, state_maps):
    with patch('polaris.analytics.publish.publish'):
        client = Client(schema)
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
                        stateMaps=state_maps
                    )
                ]
            )
        )
                                  )
        assert 'data' in response
        result = response['data']['updateProjectStateMaps']
        assert result
        assert result['success']


class TestRebuildDeliveryCyclesFlag:
    def it_rebuilds_delivery_cycles(self, work_items_delivery_cycles_setup):
        # example case to test:
        # when there was a work item in state done (mapped to complete)
        # corresponding delivery cycle would have lead time and end_date as null
        # when done is mapped to closed in new mapping, the delivery cycle should have a lead time calculated and end_date set

        project = work_items_delivery_cycles_setup
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id

        current_delivery_cycle_id = project.work_items_sources[0].work_items[0].current_delivery_cycle_id

        message = fake_send(
            RecalculateCycleMetricsForWorkItemSource(
                send=dict(
                    project_key=project_key,
                    work_items_source_key=work_items_source_key,
                    rebuild_delivery_cycles=True
                )
            )
        )
        channel = mock_channel()
        publisher = mock_publisher()

        AnalyticsTopicSubscriber(channel, publisher=publisher).dispatch(channel, message)

        assert db.connection().execute(
            f"select delivery_cycle_id from analytics.work_item_delivery_cycles\
                     where work_item_delivery_cycles.work_item_id='{work_item_id}'").scalar() != current_delivery_cycle_id

    def it_does_not_rebuild_delivery_cycles(self, work_items_delivery_cycles_setup):
        # example case to test:
        # when there was a work item in state done (mapped to complete)
        # corresponding delivery cycle would have lead time and end_date as null
        # when done is mapped to closed in new mapping, the delivery cycle should have a lead time calculated and end_date set

        project = work_items_delivery_cycles_setup
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id

        current_delivery_cycle_id = project.work_items_sources[0].work_items[0].current_delivery_cycle_id

        message = fake_send(
            RecalculateCycleMetricsForWorkItemSource(
                send=dict(
                    project_key=project_key,
                    work_items_source_key=work_items_source_key,
                    rebuild_delivery_cycles=False
                )
            )
        )
        channel = mock_channel()
        publisher = mock_publisher()

        AnalyticsTopicSubscriber(channel, publisher=publisher).dispatch(channel, message)

        assert db.connection().execute(
            f"select delivery_cycle_id from analytics.work_item_delivery_cycles\
                     where work_item_delivery_cycles.work_item_id='{work_item_id}'").scalar() == current_delivery_cycle_id


class TestRebuildDeliveryCycles:

    def it_recomputes_delivery_cycle_durations(self, work_items_delivery_cycles_setup):
        # example case to test:
        # when there was a work item in state done (mapped to complete)
        # corresponding delivery cycle would have lead time and end_date as null
        # when done is mapped to closed in new mapping, the delivery cycle should have a lead time calculated and end_date set

        project = work_items_delivery_cycles_setup
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id

        fake_dispatch_message(project_key, work_items_source_key)

        assert db.connection().execute(
            f"select sum(cumulative_time_in_state) from "
            f"analytics.work_item_delivery_cycle_durations inner join analytics.work_item_delivery_cycles on work_item_delivery_cycles.delivery_cycle_id = work_item_delivery_cycle_durations.delivery_cycle_id\
                             where work_item_delivery_cycles.work_item_id='{work_item_id}'").scalar() > 0

    def it_sets_the_current_delivery_cycle_id_of_workitems(self, work_items_delivery_cycles_setup):
        # example case to test:
        # when there was a work item in state done (mapped to complete)
        # corresponding delivery cycle would have lead time and end_date as null
        # when done is mapped to closed in new mapping, the delivery cycle should have a lead time calculated and end_date set

        project = work_items_delivery_cycles_setup
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id

        old_delivery_cycle_id = project.work_items_sources[0].work_items[0].current_delivery_cycle_id

        fake_dispatch_message(project_key, work_items_source_key)

        assert db.connection().execute(
            f"select current_delivery_cycle_id from analytics.work_items\
                             where work_items.id='{work_item_id}'").scalar() != old_delivery_cycle_id

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

        update_project_state_maps(
            project_key,
            work_items_source_key,
            state_maps=[
                dict(state="created", stateType=WorkItemsStateType.open.value),
                dict(state="doing", stateType=WorkItemsStateType.wip.value),
                dict(state="done", stateType=WorkItemsStateType.closed.value),
            ])

        fake_dispatch_message(project_key, work_items_source_key)

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


    def it_updates_commit_stats_for_recreated_delivery_cycles(self, project_work_items_commits_fixture):
        project = project_work_items_commits_fixture
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
        update_project_state_maps(
            project_key,
            work_items_source_key,
            state_maps=[
                dict(state="created", stateType=WorkItemsStateType.open.value),
                dict(state="doing", stateType=WorkItemsStateType.wip.value),
                dict(state="done", stateType=WorkItemsStateType.closed.value),
            ]
        )

        fake_dispatch_message(project_key, work_items_source_key)
        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycles\
                                     where work_item_delivery_cycles.work_item_id='{work_item_id}' and repository_count=3 and commit_count=3 and latest_commit='2020-01-07 00:00:00.000000' and earliest_commit='2020-01-05 00:00:00.000000'").scalar() == 1

    def it_updates_implementation_effort_for_recreated_delivery_cycles(self, project_work_items_commits_fixture):

        project = project_work_items_commits_fixture
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
        update_project_state_maps(
            project_key,
            work_items_source_key,
            state_maps=[
                dict(state="created", stateType=WorkItemsStateType.open.value),
                dict(state="doing", stateType=WorkItemsStateType.wip.value),
                dict(state="done", stateType=WorkItemsStateType.closed.value),
            ]
        )
        fake_dispatch_message(project_key, work_items_source_key)
        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycles\
                                     where work_item_delivery_cycles.work_item_id='{work_item_id}' and effort=3"
        ).scalar() == 1

    def it_updates_commit_stats_for_recreated_delivery_cycles_for_multiple_closed_states(self,
                                                                                         project_work_items_commits_fixture):
        project = project_work_items_commits_fixture
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
        update_project_state_maps(
            project_key,
            work_items_source_key,
            state_maps=[
                dict(state="created", stateType=WorkItemsStateType.open.value),
                dict(state="doing", stateType=WorkItemsStateType.closed.value),
                dict(state="done", stateType=WorkItemsStateType.closed.value),
            ]
        )
        fake_dispatch_message(project_key, work_items_source_key)
        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycles\
                                             where work_item_delivery_cycles.work_item_id='{work_item_id}' and repository_count=3 and commit_count=3 and latest_commit='2020-01-07 00:00:00.000000' and earliest_commit='2020-01-05 00:00:00.000000'").scalar() == 1



class TestComputeImplementationComplexityMetricsOnUpdateStateMaps:

    def it_recomputes_complexity_metrics_for_recreated_delivery_cycles(self, project_work_items_commits_fixture):
        project = project_work_items_commits_fixture
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
        update_project_state_maps(
            project_key,
            work_items_source_key,
            state_maps=[
                dict(state="created", stateType=WorkItemsStateType.open.value),
                dict(state="doing", stateType=WorkItemsStateType.wip.value),
                dict(state="done", stateType=WorkItemsStateType.closed.value),
            ]
        )
        fake_dispatch_message(project_key, work_items_source_key)
        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycles where work_item_id={work_item_id} and total_lines_changed_merge=8 \
            and total_files_changed_merge=1 and average_lines_changed_merge=8 and total_lines_changed_non_merge=16 and \
            total_files_changed_non_merge=2 and total_lines_deleted_non_merge=8 \
            and total_lines_inserted_non_merge=8").scalar() == 1

    def it_recomputes_complexity_metrics_for_recreated_delivery_cycles_for_multiple_closed_states(self,
                                                                                                  project_work_items_commits_fixture):
        project = project_work_items_commits_fixture
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        work_item_id = project.work_items_sources[0].work_items[0].id
        update_project_state_maps(
            project_key,
            work_items_source_key,
            state_maps=[
                dict(state="created", stateType=WorkItemsStateType.open.value),
                dict(state="doing", stateType=WorkItemsStateType.closed.value),
                dict(state="done", stateType=WorkItemsStateType.closed.value),
            ]
        )
        fake_dispatch_message(project_key, work_items_source_key)
        assert db.connection().execute(
            f"select count(*) from analytics.work_item_delivery_cycles where work_item_id={work_item_id} and total_lines_changed_merge=0 \
                    and total_files_changed_merge=0 and average_lines_changed_merge=0 and total_lines_changed_non_merge=8 and \
                    total_files_changed_non_merge=1 and total_lines_deleted_non_merge=4 \
                    and total_lines_inserted_non_merge=4").scalar() == 1



class TestComputeContributorMetricsOnUpdateStateMaps:

    def it_recomputes_contributor_metrics_for_recreated_delivery_cycles(self, project_work_items_commits_fixture):

        project = project_work_items_commits_fixture
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        update_project_state_maps(
            project_key,
            work_items_source_key,
            state_maps=[
                dict(state="created", stateType=WorkItemsStateType.open.value),
                dict(state="doing", stateType=WorkItemsStateType.wip.value),
                dict(state="done", stateType=WorkItemsStateType.closed.value),
            ]
        )
        fake_dispatch_message(project_key, work_items_source_key)
        # single author/committer, 3 commits with 8 lines in each, 1 delivery cycle
        assert db.connection().execute(
            f"select count(delivery_cycle_id) from analytics.work_item_delivery_cycle_contributors \
            where total_lines_as_author=16 and total_lines_as_reviewer=8").scalar() == 1



class TestPopulateWorkItemSourceFileChangesOnUpdateStateMaps:

    def it_populates_work_item_source_file_changes_for_recreated_delivery_cycles(self,
                                                                                 project_work_items_commits_fixture):

        project = project_work_items_commits_fixture
        project_key = str(project.key)
        work_items_source_key = project.work_items_sources[0].key
        update_project_state_maps(
            project_key,
            work_items_source_key,
            state_maps=[
                dict(state="created", stateType=WorkItemsStateType.open.value),
                dict(state="doing", stateType=WorkItemsStateType.wip.value),
                dict(state="done", stateType=WorkItemsStateType.closed.value),
            ]
        )
        fake_dispatch_message(project_key, work_items_source_key)
        # 2 source files each, for 3 commit, 1 work item
        assert db.connection().execute(
            "select count(distinct (source_file_id, commit_id, work_item_id)) from analytics.work_item_source_file_changes where delivery_cycle_id is not NULL").scalar() == 6
