# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.messaging.topics import TopicSubscriber, CommandsTopic
from polaris.analytics.messaging.commands import \
    ResolveCommitsWorkItems

class CommandsTopicSubscriber(TopicSubscriber):
    def __init__(self, channel):
        super().__init__(
            topic = CommandsTopic(channel, create=True),
            subscriber_queue='commands_analytics',
            message_classes=[
            ],
            exclusive=False,
            no_ack=True
        )

    def dispatch(self, channel, message):
        if ResolveCommitsWorkItems.message_type == message.message_type:
            return self.process_resolve_commits_work_items(channel, message)


    @staticmethod
    def process_resolve_commits_work_items(channel, message):
        pass

