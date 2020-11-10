# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


import uuid
from datetime import datetime
from polaris.analytics.messaging.subscribers import AnalyticsTopicSubscriber
from polaris.messaging.messages import PullRequestsUpdated
from polaris.messaging.test_utils import mock_channel, fake_send, mock_publisher
from polaris.messaging.topics import AnalyticsTopic
from polaris.analytics.messaging.commands import ResolveWorkItemsForPullRequests

from test.fixtures.repo_org import rails_organization_key, rails_repository_key


class TestPullRequestsUpdated:


    def it_publishes_responses_correctly(self):
        pull_request_summaries = [
            dict(
                key=uuid.uuid4(),
                source_id='1000',
                display_id='178',
                title="PO-178 Graphql API updates.",
                description="PO-178",
                source_state="open",
                state="open",
                created_at=datetime.strptime("2020-06-18 01:32:00.553000", "%Y-%m-%d %H:%M:%S.%f"),
                updated_at=datetime.strptime("2020-06-23 01:53:48.171000", "%Y-%m-%d %H:%M:%S.%f"),
                merge_status="can_be_merged",
                end_date=datetime.strptime("2020-06-11 18:57:08.818000", "%Y-%m-%d %H:%M:%S.%f"),
                source_branch="PO-178",
                target_branch="master",
                source_repository_key=rails_repository_key,
                web_url="https://gitlab.com/polaris-services/polaris-analytics-service/-/merge_requests/69"
            )
        ]
        message = fake_send(
            PullRequestsUpdated(
                send=dict(
                    organization_key=rails_organization_key,
                    repository_key=rails_repository_key,
                    pull_request_summaries=pull_request_summaries
                )
            )
        )
        publisher = mock_publisher()
        channel = mock_channel()

        result = AnalyticsTopicSubscriber(channel, publisher=publisher).dispatch(channel, message)
        publisher.assert_topic_called_with_message(AnalyticsTopic, ResolveWorkItemsForPullRequests)

