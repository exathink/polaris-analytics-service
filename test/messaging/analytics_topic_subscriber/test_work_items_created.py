# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from unittest.mock import patch
from polaris.analytics.messaging.subscribers import AnalyticsTopicSubscriber
from polaris.messaging.messages import WorkItemsCreated, WorkItemsCommitsResolved
from polaris.messaging.test_utils import mock_channel, fake_send, assert_is_valid_message

from test.fixtures.work_item_commit_resolution import *


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

    def it_returns_a_valid_response(self, work_items_commits_fixture):
        work_items_source, new_work_items = work_items_commits_fixture
        message = fake_send(
            WorkItemsCreated(
                send=dict(
                    organization_key=test_organization_key,
                    work_items_source_key=work_items_source.key,
                    new_work_items=new_work_items
                )
            )
        )
        channel = mock_channel()
        response = AnalyticsTopicSubscriber(channel).dispatch(channel, message)
        assert_is_valid_message(WorkItemsCommitsResolved, response)

    def it_publishes_responses_correctly(self, work_items_commits_fixture):
        work_items_source, new_work_items = work_items_commits_fixture
        message = fake_send(
            WorkItemsCreated(
                send=dict(
                    organization_key=test_organization_key,
                    work_items_source_key=work_items_source.key,
                    new_work_items=new_work_items
                )
            )
        )
        channel = mock_channel()
        with patch('polaris.messaging.topics.AnalyticsTopic.publish') as analytics_publish:
            work_items_commits_resolved_message = AnalyticsTopicSubscriber(channel).dispatch(channel, message)
            analytics_publish.assert_called_with(work_items_commits_resolved_message)

