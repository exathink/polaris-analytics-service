# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
from polaris.messaging.utils import raise_on_failure
from polaris.messaging.topics import TopicSubscriber, VcsTopic, AnalyticsTopic
from polaris.messaging.messages import RepositoriesImported, PullRequestsCreated, PullRequestsUpdated
from polaris.analytics.db import api

logger = logging.getLogger('polaris.analytics.messaging.VcsTopicSubscriber')


class VcsTopicSubscriber(TopicSubscriber):
    def __init__(self, channel, publisher=None):
        super().__init__(
            topic=VcsTopic(channel, create=True),
            subscriber_queue='vcs_analytics',
            message_classes=[
                # Events
                RepositoriesImported,
                PullRequestsCreated,
                PullRequestsUpdated

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
        if PullRequestsCreated.message_type == message.message_type:
            result = self.process_pull_requests_created(message)
            if result['success']:
                logger.info(f"Imported {result} new and updated pull requests")

                response = PullRequestsCreated(send=message.dict, in_response_to=message)
                self.publish(AnalyticsTopic, response, channel=channel)

        if PullRequestsUpdated.message_type == message.message_type:
            result = self.process_pull_requests_updated(message)
            if result['success']:
                logger.info(f"Imported {result} new and updated pull requests")

                response = PullRequestsUpdated(send=message.dict, in_response_to=message)
                self.publish(AnalyticsTopic, response, channel=channel)

    @staticmethod
    def process_repositories_imported(message):
        logger.info(f"Organization Key: {message['organization_key']}")

        return raise_on_failure(
            message,
            api.import_repositories(
                organization_key=message['organization_key'],
                repository_summaries=message['imported_repositories']
            )
        )

    @staticmethod
    def process_pull_requests_created(message):
        logger.info(f"Repository Key: {message['repository_key']}")

        return raise_on_failure(
            message,
            api.import_new_pull_requests(
                message['repository_key'],
                message['pull_request_summaries']
            )
        )

    @staticmethod
    def process_pull_requests_updated(message):
        logger.info(f"Repository Key: {message['repository_key']}")

        return raise_on_failure(
            message,
            api.update_pull_requests(
                message['repository_key'],
                message['pull_request_summaries']
            )
        )
