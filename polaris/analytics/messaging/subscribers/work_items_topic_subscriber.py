# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging

from polaris.messaging.messages import WorkItemsCommitsResolved, ImportWorkItems, WorkItemsCommitsUpdated
from polaris.messaging.topics import TopicSubscriber, WorkItemsTopic


logger = logging.getLogger('polaris.analytics.work_items_topic_subscriber')

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
            exclusive=False
        )

    def dispatch(self, channel, message ):
        pass