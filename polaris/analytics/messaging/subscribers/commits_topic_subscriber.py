# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from polaris.analytics.messaging.message_listener import logger
from polaris.messaging.messages import CommitHistoryImported
from polaris.messaging.topics import TopicSubscriber, CommitsTopic, WorkItemsTopic
from polaris.analytics.db import api


class CommitsTopicSubscriber(TopicSubscriber):
    def __init__(self, channel):
        super().__init__(
            topic = CommitsTopic(channel, create=True),
            subscriber_queue='commits_analytics',
            message_classes=[
                CommitHistoryImported
            ],
            exclusive=False,
            no_ack=True
        )


    def dispatch(self, channel, message):
        if CommitHistoryImported.message_type == message.message_type:
            resolved = self.process_commit_history_imported(message)
            if resolved:
               pass




    @staticmethod
    def process_commit_history_imported(message):
        organization_key = message['organization_key']
        repository_name = message['repository_name']

        logger.info(f'Processing {message.message_type} for organization {organization_key} repository {repository_name}')
        if len(message['new_commits']) > 0:
            result = api.import_new_commits(
                organization_key=organization_key,
                repository_key=message['repository_key'],
                new_commits = message['new_commits'],
                new_contributors = message['new_contributors']
            )

        else:
            logger.info(f" {message['total_commits']} total commits. No new commits")

