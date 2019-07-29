# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
from polaris.messaging.topics import TopicSubscriber, VcsTopic, AnalyticsTopic
from polaris.messaging.messages import RepositoriesImported
from polaris.analytics.db import api

logger = logging.getLogger('polaris.analytics.messaging.VcsTopicSubscriber')

class VcsTopicSubscriber(TopicSubscriber):
    def __init__(self, channel, publisher=None):
        super().__init__(
            topic=VcsTopic(channel, create=True),
            subscriber_queue='vcs_analytics',
            message_classes=[
                # Events
                RepositoriesImported

            ],
            publisher=publisher,
            exclusive=False
        )

    def dispatch(self, channel, message):
        if RepositoriesImported.message_type == message.message_type:
            result = self.process_repositories_imported(message)
            if result['success']:
                logger.info(f"Imported {result['imported']} new repositories")

                response = RepositoriesImported(send=message.dict, in_response_to=message)
                self.publish(AnalyticsTopic, response, channel=channel)


    @staticmethod
    def process_repositories_imported(message):
        logger.info(f"Organization Key: {message['organization_key']}")

        return api.import_repositories(
            organization_key=message['organization_key'],
            repository_summaries=message['imported_repositories']
        )


