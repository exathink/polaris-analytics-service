# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from unittest.mock import patch
from polaris.analytics.messaging.subscribers import AnalyticsTopicSubscriber
from polaris.messaging.messages import WorkItemsCommitsResolved
from polaris.messaging.topics import AnalyticsTopic
from polaris.analytics.messaging.commands import UpdateCommitsWorkItemsSummaries
from polaris.messaging.test_utils import mock_channel, fake_send, mock_publisher

from test.fixtures.work_item_commit_resolution import *


@pytest.fixture
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
    test_commits = [
        dict(
            repository_id=test_repo.id,
            key=test_commit_key.hex,
            source_commit_id=test_commit_source_id,
            commit_message="Another change. Fixes issue #1000",
            author_date=get_date("2018-12-03"),
            **commits_common_fields(commits_fixture)
        )
    ]
    create_test_commits(test_commits)
    yield new_key, test_commit_key, work_item_source.key, test_repo.key


class TestUpdateCommitsWorkItemsSummaries:

    def it_returns_a_valid_response(self, work_items_commits_fixture):
        work_item_key, commit_key, work_items_source_key, repository_key = work_items_commits_fixture
        message = fake_send(UpdateCommitsWorkItemsSummaries(
            send=dict(
                organization_key=test_organization_key,
                work_items_commits=[
                    dict(
                        work_item_key=work_item_key,
                        commit_key=commit_key,
                        work_items_source_key=work_items_source_key,
                        repository_key=repository_key
                    )
                ]
            )
        ))
        publisher = mock_publisher()
        channel = mock_channel()
        result = AnalyticsTopicSubscriber(channel, publisher=publisher).dispatch(channel, message)
        assert result['success']


