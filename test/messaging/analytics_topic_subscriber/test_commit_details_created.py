# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from unittest.mock import patch
from polaris.analytics.messaging.subscribers import AnalyticsTopicSubscriber
from polaris.messaging.messages import CommitDetailsCreated
from polaris.messaging.test_utils import mock_channel, fake_send, assert_is_valid_message

from test.fixtures.commit_details import *

class TestDispatchCommtDetailsCreated:

    def it_returns_a_valid_response(self, commit_details_imported_payload, cleanup):
        payload = commit_details_imported_payload
        message = fake_send(CommitDetailsCreated(send=payload))
        channel = mock_channel()
        result = AnalyticsTopicSubscriber(channel).dispatch(channel, message)
        assert result

