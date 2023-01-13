# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
from polaris.messaging.topics import TopicSubscriber, AnalyticsTopic
from polaris.messaging.messages import \
    CommitsCreated, CommitDetailsCreated, \
    WorkItemsCreated, WorkItemsUpdated, \
    WorkItemsCommitsResolved, \
    ProjectsRepositoriesAdded, RepositoriesImported, PullRequestsCreated, PullRequestsUpdated



from polaris.analytics.messaging.commands import UpdateCommitsWorkItemsSummaries, \
    InferProjectsRepositoriesRelationships, ResolveWorkItemsSourcesForRepositories, \
    UpdateWorkItemsCommitsStats, ComputeImplementationComplexityMetricsForWorkItems, \
    RegisterSourceFileVersions, ComputeImplementationComplexityMetricsForCommits, \
    ComputeContributorMetricsForCommits, ComputeContributorMetricsForWorkItems, \
    PopulateWorkItemSourceFileChangesForCommits, PopulateWorkItemSourceFileChangesForWorkItems, \
    ResolveCommitsForWorkItems, ResolvePullRequestsForWorkItems, ResolveWorkItemsForPullRequests, \
    ResolveWorkItemsForCommits, ResolveTeamsForWorkItems, RecalculateCycleTimesForWorkItemSource


from polaris.analytics.messaging.messages import ContributorTeamAssignmentsChanged

from polaris.messaging.utils import raise_on_failure

from polaris.analytics.db import api, commands

logger = logging.getLogger('polaris.analytics.analytics_topic_subscriber')


class AnalyticsTopicSubscriber(TopicSubscriber):
    def __init__(self, channel, publisher=None):
        super().__init__(
            topic=AnalyticsTopic(channel, create=True),
            subscriber_queue='analytics_analytics',
            message_classes=[
                RepositoriesImported,
                CommitsCreated,
                CommitDetailsCreated,
                WorkItemsCreated,
                WorkItemsCommitsResolved,
                # Commands
                UpdateCommitsWorkItemsSummaries,
                InferProjectsRepositoriesRelationships,
                ResolveWorkItemsSourcesForRepositories,
                UpdateWorkItemsCommitsStats,
                ComputeImplementationComplexityMetricsForWorkItems,
                ComputeImplementationComplexityMetricsForCommits,
                RegisterSourceFileVersions,
                ComputeContributorMetricsForWorkItems,
                ComputeContributorMetricsForCommits,
                PopulateWorkItemSourceFileChangesForCommits,
                PopulateWorkItemSourceFileChangesForWorkItems,
                ResolveCommitsForWorkItems,
                PullRequestsCreated,
                PullRequestsUpdated,
                ResolveWorkItemsForPullRequests,
                ResolvePullRequestsForWorkItems,
                ResolveWorkItemsForCommits,
                ResolveTeamsForWorkItems,
                RecalculateCycleTimesForWorkItemSource,
                # Internal messages
                ContributorTeamAssignmentsChanged
            ],
            publisher=publisher,
            exclusive=False
        )

    def dispatch(self, channel, message):
        logger.info(f"Dispatching {message.message_type}")
        # Messages
        if CommitsCreated.message_type == message.message_type:
            resolve_work_items_for_commits_command = ResolveWorkItemsForCommits(
                send=message.dict,
                in_response_to=message
            )
            self.publish(AnalyticsTopic, resolve_work_items_for_commits_command)

        elif WorkItemsCreated.message_type == message.message_type:
            resolve_commits_for_work_items_command = ResolveCommitsForWorkItems(
                send=message.dict,
                in_response_to=message
            )
            self.publish(AnalyticsTopic, resolve_commits_for_work_items_command)

            resolve_pull_requests_for_work_items_command = ResolvePullRequestsForWorkItems(
                send=message.dict,
                in_response_to=message
            )
            self.publish(AnalyticsTopic, resolve_pull_requests_for_work_items_command)

            return resolve_commits_for_work_items_command, resolve_pull_requests_for_work_items_command

        elif WorkItemsUpdated.message_type == message.message_type:
            # Need this extra mapping here since the original assumption was this
            # would be called only for new work items. We are now extending this call
            # to all updates to give more chances for work item commit resolution to
            # process in case resolveWorkItemsForCommits fails for any reason on any single
            # commit.
            resolve_commits_for_work_items_command = ResolveCommitsForWorkItems(
                send=dict(
                    organization_key=message['organization_key'],
                    work_items_source_key=message['work_items_source_key'],
                    new_work_items=message['updated_work_items']
                ),
                in_response_to=message
            )
            self.publish(AnalyticsTopic, resolve_commits_for_work_items_command)

            resolve_pull_requests_for_work_items_command = ResolvePullRequestsForWorkItems(
                send=dict(
                    organization_key=message['organization_key'],
                    work_items_source_key=message['work_items_source_key'],
                    new_work_items=message['updated_work_items']
                ),
                in_response_to=message
            )
            self.publish(AnalyticsTopic, resolve_pull_requests_for_work_items_command)

            return resolve_commits_for_work_items_command, resolve_pull_requests_for_work_items_command

        elif PullRequestsCreated.message_type == message.message_type:
            resolve_work_items_for_pull_requests_command = ResolveWorkItemsForPullRequests(
                send=message.dict,
                in_response_to=message
            )
            self.publish(AnalyticsTopic, resolve_work_items_for_pull_requests_command)

        elif PullRequestsUpdated.message_type == message.message_type:
            # TODO: Using the same resolve method as new prs. Discuss if we need another one,\
            #  particularly if there is a scenario when we need to remove old mappings \
            #  (not sure if this is already taken care of)
            resolve_work_items_for_pull_requests_command = ResolveWorkItemsForPullRequests(
                send=message.dict,
                in_response_to=message
            )
            self.publish(AnalyticsTopic, resolve_work_items_for_pull_requests_command)

        elif CommitDetailsCreated.message_type == message.message_type:
            #  register_source_file_versions
            register_source_file_versions_command = RegisterSourceFileVersions(
                send=message.dict,
                in_response_to=message
            )
            self.publish(AnalyticsTopic, register_source_file_versions_command)

            # Publish a sub command to compute following complexity metrics:
            # 1. Commit stats for merge commits
            # 2. Commit stats for non merge commits
            compute_implementation_complexity_metrics_for_commits_command = ComputeImplementationComplexityMetricsForCommits(
                send=message.dict,
                in_response_to=message
            )
            self.publish(AnalyticsTopic, compute_implementation_complexity_metrics_for_commits_command)

            compute_contributor_metrics_for_commits_command = ComputeContributorMetricsForCommits(
                send=message.dict,
                in_response_to=message
            )
            self.publish(AnalyticsTopic, compute_contributor_metrics_for_commits_command)

            populate_work_item_source_file_changes_for_commits_command = PopulateWorkItemSourceFileChangesForCommits(
                send=message.dict,
                in_response_to=message
            )
            self.publish(AnalyticsTopic, populate_work_item_source_file_changes_for_commits_command)

            return register_source_file_versions_command, compute_implementation_complexity_metrics_for_commits_command, \
                compute_contributor_metrics_for_commits_command, populate_work_item_source_file_changes_for_commits_command

        elif WorkItemsCommitsResolved.message_type == message.message_type:
            logger.info(f'Processing message {WorkItemsCommitsResolved.message_type}')
            # Publish a sub command to update commit work items summaries
            update_commit_work_items_summaries_command = UpdateCommitsWorkItemsSummaries(
                send=message.dict,
                in_response_to=message
            )
            self.publish(AnalyticsTopic, update_commit_work_items_summaries_command)

            # Publish a sub command to infer project repositories relationships
            infer_projects_repositories_relationships = InferProjectsRepositoriesRelationships(
                send=message.dict
            )
            self.publish(AnalyticsTopic, infer_projects_repositories_relationships)

            resolve_teams_for_work_items_command = ResolveTeamsForWorkItems(
                send=message.dict
            )
            self.publish(AnalyticsTopic, resolve_teams_for_work_items_command)

            # Publish a sub command to compute following stats for each delivery cycle:
            # 1. Work items commits span
            # 2. Work items commits repository count
            # 3. Work Items commits commit count
            update_work_items_commits_stats_command = UpdateWorkItemsCommitsStats(
                send=message.dict,
                in_response_to=message
            )
            self.publish(AnalyticsTopic, update_work_items_commits_stats_command)

            # Publish a sub command to compute following complexity metrics:
            # 1. Commit stats for merge commits
            # 2. Commit stats for non merge commits
            compute_implementation_complexity_metrics_for_work_items_command = ComputeImplementationComplexityMetricsForWorkItems(
                send=message.dict,
                in_response_to=message
            )
            self.publish(AnalyticsTopic, compute_implementation_complexity_metrics_for_work_items_command)

            compute_contributor_metrics_for_work_items_command = ComputeContributorMetricsForWorkItems(
                send=message.dict,
                in_response_to=message
            )
            self.publish(AnalyticsTopic, compute_contributor_metrics_for_work_items_command)

            populate_work_item_source_file_changes_for_work_items_command = PopulateWorkItemSourceFileChangesForWorkItems(
                send=message.dict,
                in_response_to=message
            )
            self.publish(AnalyticsTopic, populate_work_item_source_file_changes_for_work_items_command)

            return update_commit_work_items_summaries_command, infer_projects_repositories_relationships, \
                resolve_teams_for_work_items_command, update_work_items_commits_stats_command, compute_implementation_complexity_metrics_for_work_items_command, \
                compute_contributor_metrics_for_work_items_command, populate_work_item_source_file_changes_for_work_items_command

        elif RepositoriesImported.message_type == message.message_type:
            return self.publish(
                AnalyticsTopic,
                ResolveWorkItemsSourcesForRepositories(
                    send=dict(
                        organization_key=message['organization_key'],
                        repositories=message['imported_repositories']
                    ),
                    in_response_to=message
                )
            )

        # Commands
        elif ResolveCommitsForWorkItems.message_type == message.message_type:
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

        elif ResolveWorkItemsForCommits.message_type == message.message_type:
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


        elif ResolvePullRequestsForWorkItems.message_type == message.message_type:
            return self.process_resolve_pull_requests_for_work_items(channel, message)

        elif ResolveWorkItemsForPullRequests.message_type == message.message_type:
            return self.process_resolve_work_items_for_pull_requests(channel, message)

        elif RegisterSourceFileVersions.message_type == message.message_type:
            return self.process_register_source_file_versions(channel, message)

        elif UpdateCommitsWorkItemsSummaries.message_type == message.message_type:
            return self.process_update_commits_work_items_summaries(channel, message)

        elif UpdateWorkItemsCommitsStats.message_type == message.message_type:
            return self.process_update_work_items_commits_stats(channel, message)

        elif ComputeImplementationComplexityMetricsForWorkItems.message_type == message.message_type:
            return self.process_compute_implementation_complexity_metrics_for_work_items(channel, message)

        elif ComputeImplementationComplexityMetricsForCommits.message_type == message.message_type:
            return self.process_compute_implementation_complexity_metrics_for_commits(channel, message)

        elif ComputeContributorMetricsForWorkItems.message_type == message.message_type:
            return self.process_compute_contributor_metrics_for_work_items(channel, message)

        elif ComputeContributorMetricsForCommits.message_type == message.message_type:
            return self.process_compute_contributor_metrics_for_commits(channel, message)

        elif PopulateWorkItemSourceFileChangesForWorkItems.message_type == message.message_type:
            return self.process_populate_work_item_source_file_changes_for_work_items(channel, message)

        elif ResolveTeamsForWorkItems.message_type == message.message_type:
            return self.process_resolve_teams_for_work_items(channel, message)

        elif InferProjectsRepositoriesRelationships.message_type == message.message_type:
            result = self.process_infer_projects_repositories_relationships(channel, message)
            if result['success']:
                if len(result['new_relationships']) > 0:
                    projects_repositories_added = ProjectsRepositoriesAdded(
                        send=dict(
                            organization_key=message['organization_key'],
                            projects_repositories=result['new_relationships']
                        )
                    )
                    self.publish(AnalyticsTopic, projects_repositories_added)
                    return projects_repositories_added

        elif ResolveWorkItemsSourcesForRepositories.message_type == message.message_type:
            return self.process_resolve_work_items_sources_for_repositories(channel, message)
        elif PopulateWorkItemSourceFileChangesForCommits.message_type == message.message_type:
            return self.process_populate_work_item_source_file_changes_for_commits(channel, message)

        elif ContributorTeamAssignmentsChanged.message_type == message.message_type:
            return self.process_contributor_team_assignments_changed(channel, message)

        elif RecalculateCycleTimesForWorkItemSource.message_type == message.message_type:
            return self.process_recalculate_cycle_times_for_work_items_source(channel, message)


    @staticmethod
    def process_register_source_file_versions(channel, message):
        organization_key = message['organization_key']
        repository_key = message['repository_key']
        commit_details = message['commit_details']
        logger.info(
            f'process_register_source_file_versions for organization {organization_key} repository {repository_key}'
        )

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
    def process_populate_work_item_source_file_changes_for_commits(channel, message):
        organization_key = message['organization_key']
        commit_details = message['commit_details']
        logger.info(f'process_populate_work_item_source_file_changes_for_commits '
                    f'for organization {organization_key} '
                    f'commits {commit_details} ')

        if len(commit_details) > 0:
            return raise_on_failure(
                message,
                commands.populate_work_items_source_file_changes_for_commits(
                    organization_key=organization_key,
                    commit_details=commit_details
                )
            )

    @staticmethod
    def process_populate_work_item_source_file_changes_for_work_items(channel, message):
        organization_key = message['organization_key']
        work_items_commits = message['work_items_commits']
        logger.info(f'process_populate_work_item_source_file_changes_for_work_items '
                    f'for organization {organization_key} '
                    f'work_item_commits {work_items_commits} ')

        if len(work_items_commits) > 0:
            return raise_on_failure(
                message,
                commands.populate_work_items_source_file_changes_for_work_items(
                    organization_key=organization_key,
                    work_items_commits=work_items_commits
                )
            )

    @staticmethod
    def process_resolve_commits_for_work_items(channel, message):
        organization_key = message['organization_key']
        work_item_source_key = message['work_items_source_key']
        new_work_items = message['new_work_items']
        logger.info(
            f'process_resolve_commits_for_work_items for organization {organization_key} '
            f'work item source {work_item_source_key} '
            f'new_work_items {new_work_items} ')

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
    def process_resolve_pull_requests_for_work_items(channel, message):
        organization_key = message['organization_key']
        work_item_source_key = message['work_items_source_key']
        new_work_items = message['new_work_items']
        logger.info(
            f'process_resolve_pull_requests_for_work_items for organization {organization_key} '
            f'work item source {work_item_source_key} '
            f'new_work_items {new_work_items} ')

        if len(new_work_items) > 0:
            return raise_on_failure(
                message,
                api.resolve_pull_requests_for_new_work_items(
                    organization_key=organization_key,
                    work_item_source_key=work_item_source_key,
                    work_item_summaries=new_work_items
                )
            )

    @staticmethod
    def process_resolve_work_items_for_pull_requests(channel, message):
        organization_key = message['organization_key']
        repository_key = message['repository_key']
        pull_request_summaries = message['pull_request_summaries']
        logger.info(
            f'process_process_resolve_work_items_for_pull_requests for organization {organization_key} '
            f'repository {repository_key} '
            f'pull_requests {pull_request_summaries} ')

        if len(pull_request_summaries) > 0:
            return raise_on_failure(
                message,
                api.resolve_work_items_for_pull_requests(
                    organization_key=organization_key,
                    repository_key=repository_key,
                    pull_request_summaries=pull_request_summaries
                )
            )

    @staticmethod
    def process_resolve_work_items_for_commits(channel, message):
        organization_key = message['organization_key']
        repository_key = message['repository_key']
        commit_summaries = message['new_commits']

        logger.info(
            f'process_resolve_work_items_for_commits for organization {organization_key} '
            f'repository {repository_key} '
            f'commits {commit_summaries} ')

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
    def process_update_commits_work_items_summaries(channel, message):
        organization_key = message['organization_key']
        work_items_commits = message['work_items_commits']

        logger.info(
            f'process_update_commits_work_items_summaries for organization {organization_key} '
            f'work_items_commits {work_items_commits} ')

        if len(work_items_commits) > 0:
            return raise_on_failure(
                message,
                commands.update_commit_work_item_summaries(organization_key, work_items_commits)
            )

    @staticmethod
    def process_update_work_items_commits_stats(channel, message):
        organization_key = message['organization_key']
        work_items_commits = message['work_items_commits']

        logger.info(
            f'process_update_work_items_commits_stats for organization {organization_key} '
            f'work_items_commits {work_items_commits} ')

        if len(work_items_commits) > 0:
            return raise_on_failure(
                message,
                commands.update_work_items_commits_stats(organization_key, work_items_commits)
            )

    @staticmethod
    def process_compute_implementation_complexity_metrics_for_work_items(channel, message):
        organization_key = message['organization_key']
        work_items_commits = message['work_items_commits']

        logger.info(
            f'process_compute_implementation_complexity_metrics_for_work_items for organization {organization_key} '
            f'work_items_commits {work_items_commits} ')

        if len(work_items_commits) > 0:
            return raise_on_failure(
                message,
                commands.compute_implementation_complexity_metrics_for_work_items(organization_key, work_items_commits)
            )

    @staticmethod
    def process_compute_implementation_complexity_metrics_for_commits(channel, message):
        organization_key = message['organization_key']
        commit_details = message['commit_details']

        logger.info(
            f'process_compute_implementation_complexity_metrics_for_commits for organization {organization_key} '
            f'commit_details {commit_details} ')

        if len(commit_details) > 0:
            return raise_on_failure(
                message,
                commands.compute_implementation_complexity_metrics_for_commits(organization_key, commit_details)
            )

    @staticmethod
    def process_compute_contributor_metrics_for_work_items(channel, message):
        organization_key = message['organization_key']
        work_items_commits = message['work_items_commits']

        logger.info(
            f'process_compute_contributor_metrics_for_work_items for organization {organization_key} '
            f'work_items_commits {work_items_commits} ')

        if len(work_items_commits) > 0:
            return raise_on_failure(
                message,
                commands.compute_contributor_metrics_for_work_items(organization_key, work_items_commits)
            )

    @staticmethod
    def process_compute_contributor_metrics_for_commits(channel, message):
        organization_key = message['organization_key']
        repository_name = message['repository_name']
        repository_key = message['repository_key']
        commit_details = message['commit_details']

        logger.info(
            f'process_compute_contributor_metrics_for_commits for '
            f'organization {organization_key} '
            f'repository {repository_name}({repository_key})'
        )

        if len(commit_details) > 0:
            return raise_on_failure(
                message,
                commands.compute_contributor_metrics_for_commits(organization_key, commit_details)
            )

    @staticmethod
    def process_infer_projects_repositories_relationships(channel, message):
        organization_key = message['organization_key']
        work_items_commits = message['work_items_commits']
        logger.info(f'process_infer_projects_repositories_relationships '
                    f'for organization {organization_key} '
                    f'work_items_commits {work_items_commits} ')

        if len(work_items_commits) > 0:
            return raise_on_failure(
                message,
                commands.infer_projects_repositories_relationships(organization_key, work_items_commits)
            )

    @staticmethod
    def process_resolve_work_items_sources_for_repositories(channel, message):
        organization_key = message['organization_key']
        repositories = message['repositories']
        logger.info(f'process_resolve_work_items_sources_for_repositories '
                    f'for organization {organization_key} '
                    f'repositories: {repositories}')

        if len(repositories) > 0:
            return raise_on_failure(
                message,
                commands.resolve_work_items_sources_for_repositories(
                    organization_key,
                    repositories
                )
            )

    @staticmethod
    def process_contributor_team_assignments_changed(channel, message):
        organization_key = message['organization_key']
        assignments = message['contributor_team_assignments']

        logger.info(f"process_contributor_team_assignments_changed for {organization_key}")
        if len(assignments) > 0:
            return raise_on_failure(
                message,
                commands.assign_contributor_commits_to_teams(
                    organization_key,
                    assignments
                )
            )

    @staticmethod
    def process_resolve_teams_for_work_items(channel, message):
        organization_key = message['organization_key']
        work_items_commits = message['work_items_commits']

        logger.info(
            f'process_compute_contributor_metrics_for_work_items for organization {organization_key} '
            f'process_resolve_teams_for_work_items {work_items_commits} ')

        if len(work_items_commits) > 0:
            return raise_on_failure(
                message,
                commands.resolve_teams_for_work_items(
                    organization_key,
                    work_items_commits
                )
            )

    @staticmethod
    def process_recalculate_cycle_times_for_work_items_source(channel, message):
        project_key = message['project_key']
        work_items_source_key = message['work_items_source_key']
        rebuild_delivery_cycles = message['rebuild_delivery_cycles']
        logger.info(
            f'process recalculate cycle times for project {project_key} work items source {work_items_source_key} '
        )

        return raise_on_failure(
            message,
            commands.recalculate_cycle_times_for_work_items_source(
                work_items_source_key,
                rebuild_delivery_cycles=rebuild_delivery_cycles
            )
        )

