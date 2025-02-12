# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from polaris.analytics.messaging.subscribers import AnalyticsTopicSubscriber
from polaris.analytics.messaging.commands import InferProjectsRepositoriesRelationships
from polaris.messaging.test_utils import mock_channel, fake_send, mock_publisher
from polaris.messaging.topics import AnalyticsTopic
from polaris.messaging.messages import ProjectsRepositoriesAdded
from test.fixtures.work_item_commit_resolution import *


@pytest.fixture
def work_items_commits_fixture(commits_fixture):
    organization, projects, repositories, _ = commits_fixture
    # select project and repo so that repo is NOT currently associated with project
    test_project = projects['mercury']
    test_repo = repositories['gamma']

    # create work items in a source that is attached to the project mercury and import work items into it.
    new_key = uuid.uuid4()
    new_work_items = [
        dict(
            key=new_key,
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
        items_data=new_work_items,
        project_key=test_project.key
    )
    # create a test commit in the repo gamma that references the work items.
    test_commit_source_id = '00001'
    test_commit_key = uuid.uuid4()
    create_test_commits([
        dict(
            repository_id=test_repo.id,
            key=test_commit_key,
            source_commit_id=test_commit_source_id,
            commit_message="Another change. Fixes issue #1000",
            author_date=get_date("2018-12-03"),
            **commits_common_fields(commits_fixture)
        )
    ])

    yield new_key, test_commit_key, work_item_source.key, test_repo.key


class TestUpdateCommitsWorkItemsSummaries:

    def it_returns_a_valid_response(self, work_items_commits_fixture):
        work_item_key, commit_key, work_items_source_key, repository_key = work_items_commits_fixture
        message = fake_send(InferProjectsRepositoriesRelationships(
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
        assert result
        publisher.assert_topic_called_with_message(AnalyticsTopic, ProjectsRepositoriesAdded)


