# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
from polaris.messaging.topics import TopicSubscriber, AnalyticsTopic
from polaris.messaging.messages import CommitsCreated, CommitDetailsCreated, WorkItemsCreated, WorkItemsCommitsResolved
from polaris.messaging.utils import raise_on_failure

from polaris.analytics.db import api, aggregations

logger = logging.getLogger('polaris.analytics.analytics_topic_subscriber')

class AnalyticsTopicSubscriber(TopicSubscriber):
    def __init__(self, channel, publisher=None):
        super().__init__(
            topic = AnalyticsTopic(channel, create=True),
            subscriber_queue='analytics_analytics',
            message_classes=[
                CommitsCreated,
                CommitDetailsCreated,
                WorkItemsCreated,
                WorkItemsCommitsResolved
            ],
            publisher=publisher,
            exclusive=False
        )

    def dispatch(self, channel, message):

        if CommitsCreated.message_type == message.message_type:
            result = self.process_resolve_work_items_for_commits(channel, message)
            if result is not None and len(result['resolved']) > 0:
                response = WorkItemsCommitsResolved(
                    send=dict(
                        organization_key=message['organization_key'],
                        work_items_commits=result['resolved']
                    ),
                    in_response_to=message
                )
                self.publish(AnalyticsTopic, response)
                return response

        elif WorkItemsCreated.message_type == message.message_type:
            result = self.process_resolve_commits_for_work_items(channel, message)
            if result is not None and len(result['resolved']) > 0:
                response = WorkItemsCommitsResolved(
                    send=dict(
                        organization_key=message['organization_key'],
                        work_items_commits=result['resolved']
                    ),
                    in_response_to=message
                )
                self.publish(AnalyticsTopic, response)
                return response

        elif CommitDetailsCreated.message_type == message.message_type:
            return self.process_resolve_commit_details_created(channel, message)

        elif WorkItemsCommitsResolved.message_type == message.message_type:
            return self.process_work_items_commits_resolved(channel, message)

    @staticmethod
    def process_resolve_commit_details_created(channel, message):
        organization_key = message['organization_key']
        repository_name = message['repository_name']
        commit_details = message['commit_details']
        logger.info(
            f'Organization {organization_key} repository {repository_name}')

        if len(commit_details) > 0:
            return raise_on_failure(
                message,
                api.register_source_file_versions(
                    organization_key=organization_key,
                    repository_key=message['repository_key'],
                    commit_details=commit_details
                )
            )

    @staticmethod
    def process_resolve_commits_for_work_items(channel, message):
        organization_key = message['organization_key']
        work_item_source_key = message['work_items_source_key']
        new_work_items = message['new_work_items']
        logger.info(
            f'Process WorkItemsCreated for Organization {organization_key} work item source {work_item_source_key}')

        if len(new_work_items) > 0:
            return raise_on_failure(
                message,
                api.resolve_commits_for_new_work_items(
                    organization_key=organization_key,
                    work_item_source_key=work_item_source_key,
                    work_item_summaries=new_work_items
                )
            )

    @staticmethod
    def process_resolve_work_items_for_commits(channel, message):
        organization_key = message['organization_key']
        repository_key = message['repository_key']
        commit_summaries = message['new_commits']

        if len(commit_summaries) > 0:
            return raise_on_failure(
                message,
                api.resolve_work_items_for_commits(
                    organization_key=organization_key,
                    repository_key=repository_key,
                    commit_summaries=commit_summaries

                )
            )

    @staticmethod
    def process_work_items_commits_resolved(channel, message):
        organization_key = message['organization_key']
        work_items_commits = message['work_items_commits']

        if len(work_items_commits) > 0:
            return raise_on_failure(
                message,
                aggregations.update_commit_work_item_summaries(organization_key, work_items_commits)
            )