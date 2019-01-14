# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
from polaris.messaging.topics import TopicSubscriber, AnalyticsTopic
from polaris.messaging.messages import CommitDetailsCreated
from polaris.messaging.utils import raise_on_failure

from polaris.analytics.db import api

logger = logging.getLogger('polaris.analytics.analytics_topic_subscriber')

class AnalyticsTopicSubscriber(TopicSubscriber):
    def __init__(self, channel):
        super().__init__(
            topic = AnalyticsTopic(channel, create=True),
            subscriber_queue='analytics_analytics',
            message_classes=[
                CommitDetailsCreated
            ],
            exclusive=False
        )

    def dispatch(self, channel, message):
        if CommitDetailsCreated.message_type == message.message_type:
            return self.process_resolve_commit_details_created(channel, message)


    @staticmethod
    def process_resolve_commit_details_created(channel, message):
        organization_key = message['organization_key']
        repository_name = message['repository_name']
        commit_details = message['commit_details']
        logger.info(
            f'Processing {message.message_type} for organization {organization_key} repository {repository_name}')

        if len(commit_details) > 0:
            return raise_on_failure(
                message,
                api.register_source_file_versions(
                    organization_key=organization_key,
                    repository_key=message['repository_key'],
                    commit_details=commit_details
                )
            )

