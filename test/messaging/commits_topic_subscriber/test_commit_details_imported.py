# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from unittest.mock import patch
from polaris.messaging.messages import CommitDetailsImported, CommitDetailsCreated

from polaris.messaging.test_utils import mock_channel, fake_send, assert_is_valid_message
from polaris.analytics.messaging.subscribers import CommitsTopicSubscriber
from test.fixtures.commit_details import *

class TestDispatchCommtDetailsImported:

    def it_returns_a_valid_response(self, commit_details_imported_payload, cleanup):
        payload = commit_details_imported_payload
        message = fake_send(CommitDetailsImported(send=payload))
        channel = mock_channel()
        commit_details_created_message = CommitsTopicSubscriber(channel).dispatch(channel, message)
        assert_is_valid_message(CommitDetailsCreated, commit_details_created_message)



    def it_publishes_responses_correctly(self, commit_details_imported_payload, cleanup):
        payload = commit_details_imported_payload
        message = fake_send(CommitDetailsImported(send=payload))
        channel = mock_channel()
        with patch('polaris.messaging.topics.AnalyticsTopic.publish') as analytics_publish:
            commit_details_created_message = CommitsTopicSubscriber(channel).dispatch(channel, message)
            analytics_publish.assert_called_with(commit_details_created_message)