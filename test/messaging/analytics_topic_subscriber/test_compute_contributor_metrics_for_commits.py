# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from polaris.analytics.messaging.subscribers import AnalyticsTopicSubscriber
from polaris.analytics.messaging.commands import ComputeContributorMetricsForCommits
from polaris.messaging.test_utils import mock_channel, fake_send, mock_publisher

from test.fixtures.commit_details import *


class TestComputeContributorMetricsForCommits:

    def it_returns_a_valid_response(self, commit_details_imported_payload, cleanup):
        payload = commit_details_imported_payload
        message = fake_send(ComputeContributorMetricsForCommits(
            send=payload
        ))
        publisher = mock_publisher()
        channel = mock_channel()
        result = AnalyticsTopicSubscriber(channel, publisher=publisher).dispatch(channel, message)
        assert result['success']