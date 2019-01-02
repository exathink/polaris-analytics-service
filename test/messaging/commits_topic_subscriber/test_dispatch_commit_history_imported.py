# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from test.fixtures.commit_history_imported import *
from unittest.mock import patch
from polaris.messaging.messages import CommitHistoryImported, CommitsCreated
from polaris.analytics.messaging.commands import ResolveCommitsWorkItems

from polaris.messaging.test_utils import mock_channel, fake_send, assert_is_valid_message
from polaris.analytics.messaging.subscribers import CommitsTopicSubscriber
import pytest


@pytest.yield_fixture()
def commit_history_imported_payload():
    yield dict(
            **commit_history_imported_common,
            total_commits=1,
            new_commits=[
                dict(
                    source_commit_id='XXXX',
                    key=uuid.uuid4().hex,
                    **commit_common_fields
                )
            ],
            new_contributors=[
                dict(
                    name='Joe Blow',
                    contributor_key=joe_contributor_key,
                    alias='joe@blow.com'
                ),
                dict(
                    name='Billy Bob',
                    contributor_key=billy_contributor_key,
                    alias='billy@bob.com'
                )
            ]
        )

class TestDispatchCommitHistoryImported:

    def it_returns_a_valid_response(self, commit_history_imported_payload, cleanup):
        payload = commit_history_imported_payload
        message = fake_send(CommitHistoryImported(send=payload))
        channel = mock_channel()
        commits_created_message, resolve_work_items_command = CommitsTopicSubscriber(channel).dispatch(channel, message)
        assert_is_valid_message(CommitsCreated, commits_created_message)
        assert_is_valid_message(ResolveCommitsWorkItems, resolve_work_items_command)


    def it_publishes_the_responses_correctly(self, commit_history_imported_payload, cleanup):
        payload = commit_history_imported_payload
        message = fake_send(CommitHistoryImported(send=payload))
        channel = mock_channel()
        with patch('polaris.messaging.topics.AnalyticsTopic.publish') as analytics_publish:
            with patch('polaris.messaging.topics.CommandsTopic.publish') as commands_publish:
                commits_created_message, resolve_work_items_command = CommitsTopicSubscriber(channel).dispatch(channel,
                                                                                                               message)
                analytics_publish.assert_called_with(commits_created_message)
                commands_publish.assert_called_with(resolve_work_items_command)
