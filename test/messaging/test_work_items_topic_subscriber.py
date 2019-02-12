# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import uuid
from polaris.messaging.topics import AnalyticsTopic
from polaris.analytics.messaging.subscribers import WorkItemsTopicSubscriber
from polaris.messaging.messages import WorkItemsSourceCreated, WorkItemsCreated, WorkItemsUpdated, WorkItemsStatesChanged
from polaris.messaging.test_utils import mock_channel, fake_send, assert_topic_and_message, mock_publisher
from polaris.utils.collections import dict_merge

from test.fixtures.work_items import *


class TestWorkItemsSourceCreated:

    def it_returns_a_valid_response(self, setup_org):
        organization = setup_org
        source_key = uuid.uuid4()
        payload = dict(
            organization_key=organization.key,
            work_items_source=dict(
                name='a source',
                key=source_key,
                integration_type='github',
                commit_mapping_scope='organization',
                commit_mapping_scope_key=organization.key
            )
        )
        message = fake_send(WorkItemsSourceCreated(send=payload))
        channel = mock_channel()
        publisher = mock_publisher()

        WorkItemsTopicSubscriber(channel, publisher=publisher).dispatch(channel, message)
        publisher.assert_topic_called_with_message(AnalyticsTopic, WorkItemsSourceCreated)



class TestWorkItemsCreated:

    def it_returns_a_valid_response(self, work_items_setup):
        organization_key, work_items_source_key = work_items_setup
        payload = dict(
            organization_key=organization_key,
            work_items_source_key=work_items_source_key,
            new_work_items=[
                dict(
                    name='foo',
                    key=uuid.uuid4(),
                    display_id='1000',
                    **work_items_common()
                )
            ]
        )
        message = fake_send(WorkItemsCreated(send=payload))
        channel = mock_channel()
        publisher = mock_publisher()

        WorkItemsTopicSubscriber(channel, publisher=publisher).dispatch(channel, message)
        publisher.assert_topic_called_with_message(AnalyticsTopic, WorkItemsCreated)

class TestWorkItemsUpdated:

    def it_publishes_work_items_updated_response(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup
        payload = dict(
            organization_key=organization_key,
            work_items_source_key=work_items_source_key,
            updated_work_items=work_items
        )
        message = fake_send(WorkItemsUpdated(send=payload))
        channel = mock_channel()
        publisher = mock_publisher()

        WorkItemsTopicSubscriber(channel, publisher=publisher).dispatch(channel, message)
        publisher.assert_topic_called_with_message(AnalyticsTopic, WorkItemsUpdated)

    def it_publishes_work_item_state_change_message_when_there_are_state_changes(self, update_work_items_setup):
        organization_key, work_items_source_key, work_items = update_work_items_setup
        payload = dict(
            organization_key=organization_key,
            work_items_source_key=work_items_source_key,
            updated_work_items=[
                dict_merge(
                    work_item,
                    dict(state='foo')
                )
                for work_item in work_items
            ]
        )
        message = fake_send(WorkItemsUpdated(send=payload))
        channel = mock_channel()
        publisher = mock_publisher()

        WorkItemsTopicSubscriber(channel, publisher=publisher).dispatch(channel, message)
        publisher.assert_topic_called_with_message(AnalyticsTopic, WorkItemsUpdated, call=0)
        publisher.assert_topic_called_with_message(AnalyticsTopic, WorkItemsStatesChanged, call=1)
