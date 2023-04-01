# -*- coding: utf-8 -*-

#  Copyright (c) Exathink, LLC  2016-2023.
#  All rights reserved
#

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import uuid
from polaris.analytics.messaging.subscribers import AnalyticsTopicSubscriber
from polaris.messaging.test_utils import mock_channel, fake_send, mock_publisher
from polaris.messaging.topics import AnalyticsTopic
from polaris.analytics.messaging.commands import ProjectCustomTypeMappingsChanged
from polaris.utils.collections import Fixture, dict_merge
from polaris.analytics.db.enums import WorkItemType
from polaris.analytics.db import api
from polaris.utils.exceptions import ProcessingException


from test.fixtures.project_work_items import *

class TestProjectCustomTypeMappingsChanged(ProjectWorkItemsTest):

    class TestResponseProcessing:

        @pytest.fixture
        def setup(self, setup):
            fixture = setup
            organization = fixture.organization
            project_key=fixture.project.key
            work_items_source = fixture.work_items_source
            work_items_common = fixture.work_items_common


            test_work_items = [
                dict(
                    key=uuid.uuid4().hex,
                    name='Test Work Items',
                    display_id='PP-431',
                    **dict_merge(
                        work_items_common,
                        dict(
                            work_item_type=WorkItemType.story.value,
                            is_bug=False,
                            is_epic=False,
                            tags=['custom_type:Feature']
                        )
                    )
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Test Work Items2',
                    display_id='PP-431-1',
                    **dict_merge(
                        work_items_common,
                        dict(
                            work_item_type=WorkItemType.story.value,
                            is_bug=False,
                            is_epic=False,
                            tags=['tech-debt']
                        )
                    )
                ),
                dict(
                    key=uuid.uuid4().hex,
                    name='Test Work Items3',
                    display_id='PP-431-2',
                    **dict_merge(
                        work_items_common,
                        dict(
                            work_item_type=WorkItemType.story.value,
                            is_bug=False,
                            is_epic=False,
                            tags=['random-tag']
                        )
                    )
                )

            ]
            result = api.import_new_work_items(organization.key, work_items_source.key, test_work_items)
            assert result['success']

            result =  api.update_project_custom_type_mappings(project_key, [work_items_source.key], custom_type_mappings = [
                dict(
                    labels=['tech-debt'],
                    work_item_type=WorkItemType.task.value
                )
            ])
            assert result['success']

            yield Fixture(
                parent=fixture,
                work_items=test_work_items
            )

        def it_processes_the_message_and_updates_the_work_items(self, setup):

            fixture = setup
            project = fixture.project
            project_key = str(project.key)
            work_items_source_keys = [
                str(project.work_items_sources[0].key)
            ]

            message = fake_send(
                ProjectCustomTypeMappingsChanged(
                    send=dict(
                        project_key=project_key,
                        work_items_source_keys=work_items_source_keys,
                    )
                )
            )
            publisher = mock_publisher()
            channel = mock_channel()

            result = AnalyticsTopicSubscriber(channel, publisher=publisher).dispatch(channel, message)
            assert result['success']
            assert result['updated'] == 1

        def it_raises_errors_during_the_processing(self, setup):

            fixture = setup
            project = fixture.project
            project_key = str(project.key)
            work_items_source_keys = [
                str(project.work_items_sources[0].key)
            ]
            fake_project_key = str(uuid.uuid4())

            message = fake_send(
                ProjectCustomTypeMappingsChanged(
                    send=dict(
                        project_key=fake_project_key,
                        work_items_source_keys=work_items_source_keys,
                    )
                )
            )
            publisher = mock_publisher()
            channel = mock_channel()

            with pytest.raises(ProcessingException, match=r'Could not find project with key'):
                AnalyticsTopicSubscriber(channel, publisher=publisher).dispatch(channel, message)


