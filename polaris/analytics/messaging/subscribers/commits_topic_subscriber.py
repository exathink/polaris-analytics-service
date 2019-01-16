# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging

from polaris.messaging.utils import raise_on_failure
from polaris.messaging.messages import CommitHistoryImported, CommitsCreated, CommitDetailsImported, CommitDetailsCreated
from polaris.messaging.topics import TopicSubscriber, CommitsTopic, AnalyticsTopic, CommandsTopic
from polaris.analytics.db import api
from polaris.analytics.messaging.commands import ResolveCommitsWorkItems

logger = logging.getLogger('polaris.analytics.commits_topic_subscriber')

class CommitsTopicSubscriber(TopicSubscriber):
    def __init__(self, channel):
        super().__init__(
            topic=CommitsTopic(channel, create=True),
            subscriber_queue='commits_analytics',
            message_classes=[
                CommitHistoryImported,
                CommitDetailsImported
            ],
            exclusive=False
        )

    def dispatch(self, channel, message):
        if CommitHistoryImported.message_type == message.message_type:
            result = self.process_commit_history_imported(message)
            if result:
                commits_created_message = CommitsCreated(
                    send=dict(
                        organization_key=message['organization_key'],
                        repository_key=message['repository_key'],
                        new_commits=result['new_commits'],
                        branch=message['branch_info']['name'] if message['branch_info'] else None
                    ),
                    in_response_to=message
                )
                AnalyticsTopic(channel).publish(commits_created_message)

                resolve_work_items_command = ResolveCommitsWorkItems(
                    send=dict(
                        organization_key=message['organization_key'],
                        repository_key=message['repository_key'],
                        new_commits=result['new_commits']
                    ),
                    in_response_to=message
                )
                CommandsTopic(channel).publish(
                    resolve_work_items_command
                )
                return commits_created_message, resolve_work_items_command

        elif CommitDetailsImported.message_type == message.message_type:
            result = self.process_commit_details_imported(message)
            if result:
                commit_details_created_message = CommitDetailsCreated(
                    send=message.dict,
                    in_response_to=message
                )
                AnalyticsTopic(channel).publish(commit_details_created_message)
                return commit_details_created_message



    @staticmethod
    def process_commit_history_imported(message):

        organization_key = message['organization_key']
        repository_name = message['repository_name']
        repository_key = message['repository_key']
        logger.info(
            f'Organization {organization_key} repository {repository_name}')

        if len(message['new_commits']) > 0:
            return raise_on_failure(
                message,
                api.import_new_commits(
                    organization_key=organization_key,
                    repository_key=repository_key,
                    new_commits=message['new_commits'],
                    new_contributors=message.dict['new_contributors']
                )
            )
        else:
            logger.info(f" {message['total_commits']} total commits. No new commits")

    @staticmethod
    def process_commit_details_imported(message):
        organization_key = message['organization_key']
        repository_name = message['repository_name']
        commit_details = message['commit_details']
        logger.info(
            f'Organization {organization_key} repository {repository_name}')

        if len(commit_details) > 0:
            return raise_on_failure(
                message,
                api.import_commit_details(
                    organization_key=organization_key,
                    repository_key=message['repository_key'],
                    commit_details=commit_details
                )
            )
