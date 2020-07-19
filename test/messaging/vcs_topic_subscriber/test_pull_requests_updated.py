# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

import uuid
from test.fixtures.repo_org import *
from test.fixtures.pull_requests import pull_requests_common
from polaris.messaging.test_utils import fake_send, mock_publisher, mock_channel
from polaris.messaging.topics import AnalyticsTopic
from polaris.analytics.messaging.subscribers import VcsTopicSubscriber
from polaris.messaging.messages import PullRequestsCreated, PullRequestsUpdated
from polaris.common.enums import VcsIntegrationTypes


class TestPullRequestsUpdated:

    def it_works(self, setup_repo_org):
        repository_id, organization_id = setup_repo_org

        message = fake_send(
            PullRequestsUpdated(
                send=dict(
                    organization_key=rails_organization_key,
                    repository_key=rails_repository_key,
                    pull_request_summaries=[
                        dict(
                            key=uuid.uuid4().hex,
                            source_id=str(i),
                            display_id=str(i),
                            **pull_requests_common()
                        )
                        for i in range(0, 10)
                    ]
                )
            )
        )
        channel = mock_channel()
        publisher = mock_publisher()

        VcsTopicSubscriber(channel, publisher=publisher).dispatch(channel, message)
        publisher.assert_topic_called_with_message(AnalyticsTopic, PullRequestsUpdated)
