# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging

from polaris.analytics.db import api
from polaris.messaging.messages import WorkItemsSourceCreated, WorkItemsCreated, WorkItemsUpdated, \
    WorkItemsStatesChanged, ProjectImported, WorkItemMoved, WorkItemDeleted
from polaris.messaging.topics import TopicSubscriber, WorkItemsTopic, AnalyticsTopic
from polaris.messaging.utils import raise_on_failure

logger = logging.getLogger('polaris.analytics.work_items_topic_subscriber')


class WorkItemsTopicSubscriber(TopicSubscriber):
    def __init__(self, channel, publisher=None):
        super().__init__(
            topic=WorkItemsTopic(channel, create=True),
            subscriber_queue='work_items_analytics',
            message_classes=[
                # Events
                WorkItemsCreated,
                WorkItemsUpdated,
                WorkItemMoved,
                WorkItemDeleted,
                ProjectImported
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

        elif WorkItemsUpdated.message_type == message.message_type:
            resolved = self.process_work_items_updated(message)
            if resolved:
                work_items_updated = WorkItemsUpdated(send=message.dict, in_response_to=message)
                new_work_items = resolved.get("new_work_items")
                if new_work_items and len(new_work_items) > 0:
                    work_items_created, work_items_updated = self.extract_created_and_updated_messages(
                        message,
                        new_work_items
                    )
                    self.publish(WorkItemsTopic, work_items_created, channel=channel)

                    if work_items_updated is not None:
                        self.publish(AnalyticsTopic, work_items_updated, channel=channel)
                else:
                    self.publish(AnalyticsTopic, work_items_updated, channel=channel)

                if 'state_changes' in resolved and len(resolved['state_changes']) > 0:
                    work_items_states_changed = WorkItemsStatesChanged(
                        send=dict(
                            organization_key=message['organization_key'],
                            work_items_source_key=message['work_items_source_key'],
                            state_changes=resolved['state_changes']
                        )
                    )
                    self.publish(AnalyticsTopic, work_items_states_changed, channel=channel)

                return work_items_updated

        elif WorkItemMoved.message_type == message.message_type:
            return self.process_work_item_moved(message)

        elif WorkItemDeleted.message_type == message.message_type:
            return self.process_work_item_deleted(message)

        elif ProjectImported.message_type == message.message_type:
            logger.info('Received ProjectImported Message')
            result = self.process_project_imported(message)
            if result is not None:
                logger.info(f"{result['new_work_items_sources']} work_items_sources created")
                project_imported = ProjectImported(send=message.dict, in_response_to=message)
                self.publish(AnalyticsTopic, project_imported, channel=channel)

                return project_imported

    @staticmethod
    def extract_created_and_updated_messages(message, new_work_items):
        # If there are new work items found during create we will extract those
        # out from the existing updates and create two messages to publish

        all_work_items = message['updated_work_items']
        created_work_items = []
        updated_work_items = []

        for work_item in all_work_items:
            if work_item['key'] in new_work_items:
                created_work_items.append(work_item)
            else:
                updated_work_items.append(work_item)

        work_items_created = WorkItemsCreated(send=dict(
            organization_key=message['organization_key'],
            work_items_source_key=message['work_items_source_key'],
            new_work_items=created_work_items
        ), in_response_to=message)

        # Note: this new message only contains the actual updates.
        work_items_updated = None
        if len(updated_work_items) > 0:
            work_items_updated = WorkItemsUpdated(send=dict(
                organization_key=message['organization_key'],
                work_items_source_key=message['work_items_source_key'],
                updated_work_items=updated_work_items
            ), in_response_to=message)

        return work_items_created, work_items_updated

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
        # This is a temporary hack to resolve PO-633 in production
        # for some reason there are messages coming in without the commit_identifiers
        # might be a temporary versioning issue during last deployment.
        # the idea is that temporarily fixing these up will allow those messages to be processed.
        for work_item in updated_work_items:
            if 'commit_identifiers' not in work_item:
                logger.info(f"Empty commit_identifiers found for work item: {work_item['name']}")
                work_item['commit_identifiers'] = []

        logger.info(f"Processing  {message.message_type}: "
                    f" Organization: {organization_key}")

        return raise_on_failure(
            message,
            api.update_work_items(organization_key, work_items_source_key, updated_work_items)
        )

    @staticmethod
    def process_work_item_moved(message):
        work_item_moved = message.dict
        organization_key = work_item_moved['organization_key']
        source_work_items_source_key = work_item_moved['source_work_items_source_key']
        target_work_items_source_key = work_item_moved['target_work_items_source_key']
        work_item_data = work_item_moved['moved_work_item']
        return raise_on_failure(
            message,
            api.move_work_item(organization_key, source_work_items_source_key, target_work_items_source_key,
                               work_item_data)
        )

    @staticmethod
    def process_work_item_deleted(message):
        work_item_deleted = message.dict
        organization_key = work_item_deleted['organization_key']
        work_items_source_key = work_item_deleted['work_items_source_key']
        work_item_data = work_item_deleted['deleted_work_item']
        return raise_on_failure(
            message,
            api.delete_work_item(organization_key, work_items_source_key, work_item_data)
        )

    @staticmethod
    def process_project_imported(message):
        project_imported = message.dict
        organization_key = project_imported['organization_key']
        project_summary = project_imported['project_summary']
        logger.info(f"Processing  {message.message_type}: "
                    f" Organization: {organization_key}")

        return raise_on_failure(
            message,
            api.import_project(organization_key, project_summary)
        )
