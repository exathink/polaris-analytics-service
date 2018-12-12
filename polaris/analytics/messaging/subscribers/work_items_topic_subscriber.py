# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from polaris.analytics.messaging.message_listener import logger
from polaris.messaging.messages import WorkItemsCommitsResolved, ImportWorkItems, WorkItemsCommitsUpdated
from polaris.messaging.topics import TopicSubscriber, WorkItemsTopic
from polaris.work_tracking import work_tracker


class WorkItemsTopicSubscriber(TopicSubscriber):
    def __init__(self, channel):
        super().__init__(
            topic = WorkItemsTopic(channel, create=True),
            subscriber_queue='work_items_analytics',
            message_classes=[
                #Events
                WorkItemsCommitsResolved,
                #Commands
                ImportWorkItems

            ],
            exclusive=False,
            no_ack=True
        )

    def dispatch(self, channel, message ):
        if WorkItemsCommitsResolved.message_type == message.message_type:
            result = self.process_work_items_commits_resolved(message)
            if result:
                work_items_commits_updated_message = WorkItemsCommitsUpdated(
                    send=result,
                    in_response_to=message
                )
                WorkItemsTopic(channel).publish(message=work_items_commits_updated_message)
                return work_items_commits_updated_message



    @staticmethod
    def process_work_items_commits_resolved(message):
        work_items_commits_resolved = message.dict
        organization_key = work_items_commits_resolved['organization_key']
        repository_name = work_items_commits_resolved['repository_name']
        logger.info(f"Processing  {message.message_type}: "
                    f" Organization: {organization_key}"
                    f" Repository: {repository_name}")

        work_tracker.update_work_items_commits(organization_key, repository_name, work_items_commits_resolved['work_items_commits'])
        return work_items_commits_resolved