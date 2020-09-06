# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from unittest.mock import patch
from polaris.analytics.messaging.subscribers import AnalyticsTopicSubscriber
from polaris.messaging.messages import WorkItemsCreated, WorkItemsCommitsResolved
from polaris.messaging.test_utils import mock_channel, fake_send, mock_publisher
from polaris.messaging.topics import AnalyticsTopic
from polaris.analytics.messaging.commands import ResolveCommitsForWorkItems, ResolvePullRequestsForWorkItems

from test.fixtures.work_item_commit_resolution import *
from polaris.utils.collections import dict_merge, dict_drop


@pytest.yield_fixture
def work_items_commits_fixture(commits_fixture):
    organization, _, repositories, _ = commits_fixture
    test_repo = repositories['alpha']
    new_key = uuid.uuid4()
    new_work_items = [
        dict(
            key=new_key.hex,
            display_id='1000',
            created_at=get_date("2018-12-02"),
            **work_items_common
        )
    ]
    work_item_source = setup_work_items(
        organization,
        source_data=dict(
            integration_type='github',
            commit_mapping_scope='repository',
            commit_mapping_scope_key=test_repo.key,
            **work_items_source_common
        ),
        items_data=new_work_items
    )
    test_commit_source_id = '00001'
    test_commit_key = uuid.uuid4()
    create_test_commits([
        dict(
            repository_id=test_repo.id,
            key=test_commit_key.hex,
            source_commit_id=test_commit_source_id,
            commit_message="Another change. Fixes issue #1000",
            author_date=get_date("2018-12-03"),
            **commits_common_fields(commits_fixture)
        )
    ])
    yield work_item_source, new_work_items


class TestWorkItemsCreated:


    def it_publishes_responses_correctly(self, work_items_commits_fixture):
        work_items_source, new_work_items = work_items_commits_fixture
        message = fake_send(
            WorkItemsCreated(
                send=dict(
                    organization_key=test_organization_key,
                    work_items_source_key=work_items_source.key,
                    new_work_items=[
                        dict_merge(
                            dict_drop(work_item, ['epic_id']),
                            dict(epic_key=None)
                        )
                        for work_item in new_work_items
                    ]
                )
            )
        )
        publisher = mock_publisher()
        channel = mock_channel()

        result = AnalyticsTopicSubscriber(channel, publisher=publisher).dispatch(channel, message)
        assert len(result) == 2
        publisher.assert_topic_called_with_message(AnalyticsTopic, ResolveCommitsForWorkItems, call=0)
        publisher.assert_topic_called_with_message(AnalyticsTopic, ResolvePullRequestsForWorkItems, call=1)

