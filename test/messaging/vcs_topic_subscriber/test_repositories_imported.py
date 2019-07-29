# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import uuid
from test.fixtures.repo_org import *
from polaris.messaging.test_utils import fake_send, mock_publisher, mock_channel
from polaris.messaging.topics import AnalyticsTopic
from polaris.analytics.messaging.subscribers import VcsTopicSubscriber
from polaris.messaging.messages import RepositoriesImported
from polaris.common.enums import VcsIntegrationTypes

class TestRepositoriesImported:

    def it_works(self, setup_org):
        organization = setup_org

        message = fake_send(
            RepositoriesImported(
                send=dict(
                    organization_key=organization.key,
                    imported_repositories=[
                        dict(
                            name='A new repo',
                            key=str(uuid.uuid4()),
                            url='https://foo.bar.com',
                            description='A new new repo',
                            public=False,
                            integration_type=VcsIntegrationTypes.github.value
                        ),
                    ]
                )
            )
        )
        channel = mock_channel()
        publisher = mock_publisher()

        VcsTopicSubscriber(channel, publisher=publisher).dispatch(channel, message)
        publisher.assert_topic_called_with_message(AnalyticsTopic, RepositoriesImported)


