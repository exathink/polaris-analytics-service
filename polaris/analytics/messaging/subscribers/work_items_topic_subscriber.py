# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging

from polaris.messaging.messages import WorkItemsSourceCreated, WorkItemsCreated, WorkItemsUpdated
from polaris.messaging.topics import TopicSubscriber, WorkItemsTopic, AnalyticsTopic
from polaris.utils.collections import dict_select
from polaris.messaging.utils import raise_on_failure
from polaris.analytics.db import api

logger = logging.getLogger('polaris.analytics.work_items_topic_subscriber')


class WorkItemsTopicSubscriber(TopicSubscriber):
    def __init__(self, channel, publisher=None):
        super().__init__(
            topic = WorkItemsTopic(channel, create=True),
            subscriber_queue='work_items_analytics',
            message_classes=[
                #Events
                WorkItemsSourceCreated,
                WorkItemsCreated,
                WorkItemsUpdated,
            ],
            publisher=publisher,
            exclusive=False
        )

    def dispatch(self, channel, message):
        if WorkItemsCreated.message_type == message.message_type:
            resolved = self.process_work_items_created(message)
            if resolved:
                work_items_created = WorkItemsCreated(send=message.dict, in_response_to=message)
                self.publish(AnalyticsTopic, work_items_created, channel=channel)
                return work_items_created

        if WorkItemsUpdated.message_type == message.message_type:
            resolved = self.process_work_items_updated(message)
            if resolved:
                work_items_updated = WorkItemsUpdated(send=message.dict, in_response_to=message)
                self.publish(AnalyticsTopic, work_items_updated, channel=channel)
                return work_items_updated

        elif WorkItemsSourceCreated.message_type == message.message_type:
            result = self.process_work_items_source_created(message)
            if result is not None and result['created']:
                work_items_source_created = WorkItemsSourceCreated(send=message.dict, in_response_to=message)
                self.publish(AnalyticsTopic, work_items_source_created, channel=channel)
                return work_items_source_created


    @staticmethod
    def process_work_items_created(message):
        work_items_created = message.dict
        organization_key = work_items_created['organization_key']
        work_items_source_key = work_items_created['work_items_source_key']
        new_work_items = work_items_created['new_work_items']
        logger.info(f"Processing  {message.message_type}: "
                    f" Organization: {organization_key}")

        return raise_on_failure(
            message,
            api.import_new_work_items(organization_key, work_items_source_key, new_work_items)
        )

    @staticmethod
    def process_work_items_updated(message):
        work_items_updated = message.dict
        organization_key = work_items_updated['organization_key']
        work_items_source_key = work_items_updated['work_items_source_key']
        updated_work_items = work_items_updated['updated_work_items']
        logger.info(f"Processing  {message.message_type}: "
                    f" Organization: {organization_key}")

        return raise_on_failure(
            message,
            api.update_work_items(organization_key, work_items_source_key, updated_work_items)
        )

    @staticmethod
    def process_work_items_source_created(message):
        work_items_created = message.dict
        organization_key = work_items_created['organization_key']
        work_items_source = work_items_created['work_items_source']

        return raise_on_failure(
            message,
            api.register_work_items_source(organization_key, work_items_source)
        )

