# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.messaging.test_utils import fake_send, mock_channel, mock_publisher
from polaris.messaging.topics import AnalyticsTopic
from polaris.analytics.messaging.subscribers import AnalyticsTopicSubscriber
from polaris.analytics.messaging.commands import ResolveWorkItemsSourcesForRepositories

from test.fixtures.repository_commit_scope_resolution import *

class TestResolveWorkItemsSourcesForRepositories:

    def it_resolves_repository_commit_scope_key_for_work_items_sources(self, setup_repo_work_items_source_resolution):
        organization, work_items_source = setup_repo_work_items_source_resolution

        repository_key = uuid.uuid4()

        message = fake_send(
            ResolveWorkItemsSourcesForRepositories(
                send=dict(
                    organization_key=organization.key,
                    repositories=[
                        dict(
                            name='rails',
                            key=repository_key,
                            source_id=work_items_source.source_id,
                            url='https://foo.bar',
                            public=False,
                            integration_type='github'
                        )
                    ]
                )
            )
        )
        channel = mock_channel()
        publisher = mock_publisher()

        result = AnalyticsTopicSubscriber(channel, publisher=publisher).dispatch(channel, message)
        assert result['success']
        assert db.connection().execute(
            f"select commit_mapping_scope_key from analytics.work_items_sources where key='{work_items_source.key}'"
        ).scalar() == repository_key

