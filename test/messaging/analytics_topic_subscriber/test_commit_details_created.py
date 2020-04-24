# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from unittest.mock import patch
from polaris.analytics.messaging.subscribers import AnalyticsTopicSubscriber
from polaris.messaging.messages import CommitDetailsCreated
from polaris.messaging.topics import AnalyticsTopic
from polaris.analytics.messaging.commands import RegisterSourceFileVersions, \
    ComputeImplementationComplexityMetricsForCommits, ComputeContributorMetricsForCommits, \
    PopulateWorkItemsSourceFileChangesForCommits
from polaris.messaging.test_utils import mock_channel, fake_send, mock_publisher

from test.fixtures.commit_details import *

class TestDispatchCommitDetailsCreated:

    def it_returns_a_valid_response(self, commit_details_imported_payload, cleanup):
        payload = commit_details_imported_payload
        message = fake_send(CommitDetailsCreated(send=payload))
        publisher = mock_publisher()
        channel = mock_channel()
        result = AnalyticsTopicSubscriber(channel, publisher=publisher).dispatch(channel, message)
        assert len(result) == 4
        publisher.assert_topic_called_with_message(AnalyticsTopic, RegisterSourceFileVersions, call=0)
        publisher.assert_topic_called_with_message(AnalyticsTopic, ComputeImplementationComplexityMetricsForCommits, call=1)
        publisher.assert_topic_called_with_message(AnalyticsTopic, ComputeContributorMetricsForCommits, call=2)
        publisher.assert_topic_called_with_message(AnalyticsTopic, PopulateWorkItemsSourceFileChangesForCommits, call=3)

