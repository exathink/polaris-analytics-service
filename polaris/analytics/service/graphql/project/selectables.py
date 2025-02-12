# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from datetime import datetime, timedelta

from sqlalchemy import select, func, bindparam, distinct, and_, cast, Text, between, extract, case, literal_column, \
    union_all, literal, Date, true, or_, desc

from polaris.analytics.db.enums import WorkItemTypesToIncludeInCycleMetrics
from polaris.analytics.db.enums import WorkItemsStateType
from polaris.analytics.db.model import projects, projects_repositories, organizations, \
    repositories, contributors, \
    contributor_aliases, repositories_contributor_aliases, commits, work_items_sources, \
    work_items, work_item_state_transitions, work_items_commits, work_item_delivery_cycles, work_items_source_state_map, \
    work_item_delivery_cycle_durations, pull_requests, work_items_pull_requests, value_streams
from polaris.graphql.base_classes import NamedNodeResolver, InterfaceResolver, ConnectionResolver, \
    SelectableFieldResolver
from polaris.graphql.interfaces import NamedNode
from polaris.graphql.utils import nulls_to_zero
from polaris.utils.collections import dict_merge
from polaris.utils.datetime_utils import time_window
from polaris.utils.exceptions import ProcessingException
from .sql_expressions import select_funnel_work_items
from ..commit.sql_expressions import commit_info_columns, commits_connection_apply_filters, commit_day
from ..contributor.sql_expressions import contributor_count_apply_contributor_days_filter, \
    contributors_connection_apply_filters
from ..interfaces import \
    ProjectSetupInfo, CommitSummary, ContributorCount, RepositoryCount, OrganizationRef, CommitCount, \
    CumulativeCommitCount, CommitInfo, WeeklyContributorCount, ArchivedStatus, \
    WorkItemEventSpan, WorkItemsSourceRef, WorkItemInfo, WorkItemStateTransition, WorkItemCommitInfo, \
    FunnelViewAggregateMetrics, AggregateCycleMetrics, DeliveryCycleInfo, CycleMetricsTrends, \
    PipelineCycleMetrics, \
    TraceabilityTrends, DeliveryCycleSpan, ResponseTimeConfidenceTrends, ProjectInfo, FlowMixTrends, CapacityTrends, \
    PipelinePullRequestMetrics, PullRequestMetricsTrends, PullRequestInfo, PullRequestEventSpan, FlowRateTrends, \
    ArrivalDepartureTrends, BacklogTrends, ValueStreamInfo, Tags,Releases

from ..pull_request.sql_expressions import pull_request_info_columns, pull_requests_connection_apply_filters
from ..work_item import sql_expressions
from ..work_item.sql_expressions import work_item_events_connection_apply_time_window_filters, work_item_event_columns, \
    work_item_info_columns, work_item_commit_info_columns, work_items_connection_apply_filters, \
    work_item_delivery_cycle_info_columns, work_item_delivery_cycles_connection_apply_filters, \
    work_item_info_group_expr_columns, apply_specs_only_filter, apply_defects_only_filter, CycleMetricsTrendsBase, \
    map_work_item_type_to_flow_type, work_items_source_ref_info_columns, apply_releases_filter, apply_tags_filter
from ..utils import date_column_is_in_measurement_window, get_measurement_period, get_timeline_dates_for_trending
from ..arguments import ArrivalDepartureMetricsEnum

from polaris.common import db


class ProjectNode(NamedNodeResolver):
    interfaces = (NamedNode, ArchivedStatus, ProjectInfo)

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            projects.c.id,
            projects.c.key.label('key'),
            projects.c.name,
            projects.c.archived,
            projects.c.settings

        ]).select_from(
            projects
        ).where(projects.c.key == bindparam('key'))

class ProjectValueStreamNodes(ConnectionResolver):
    interfaces = (NamedNode, ValueStreamInfo)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        select_stmt = select([
            value_streams.c.id,
            value_streams.c.key,
            value_streams.c.name,
            value_streams.c.description,
            value_streams.c.work_item_selectors
        ]).select_from(
            projects.join(
                value_streams,
                value_streams.c.project_id == projects.c.id
            )
        ).where(projects.c.key == bindparam('key'))
        return select_stmt

class ProjectRepositoriesNodes(ConnectionResolver):
    interface = NamedNode

    @staticmethod
    def connection_nodes_selector(**kwargs):
        select_stmt = select([
            repositories.c.id,
            repositories.c.key,
            repositories.c.name,
            projects_repositories.c.excluded
        ]).select_from(
            projects.join(
                projects_repositories
            ).join(
                repositories
            )
        ).where(projects.c.key == bindparam('key'))
        if not kwargs.get('showExcluded'):
            select_stmt = select_stmt.where(
                projects_repositories.c.excluded == False
            )
        return select_stmt


class ProjectWorkItemsSourceNodes(ConnectionResolver):
    interface = NamedNode

    @staticmethod
    def connection_nodes_selector(**kwargs):
        return select([
            work_items_sources.c.id,
            work_items_sources.c.key,
            work_items_sources.c.name
        ]).select_from(
            projects.join(
                work_items_sources, work_items_sources.c.project_id == projects.c.id
            )
        ).where(projects.c.key == bindparam('key'))

    @staticmethod
    def sort_order(project_work_item_source_nodes, **kwargs):
        return [project_work_item_source_nodes.c.id.desc()]


class ProjectRecentlyActiveWorkItemsNodes(ConnectionResolver):
    interfaces = (NamedNode, WorkItemInfo, CommitCount)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        end_date = kwargs.get('before') or datetime.utcnow()
        window = time_window(begin=end_date - timedelta(days=kwargs.get('days', 7)), end=end_date)

        return select([
            work_items.c.id,
            func.min(cast(work_items.c.key, Text)).label('key'),
            func.min(work_items.c.display_id).label('name'),
            *work_item_info_group_expr_columns(work_items),
            func.count(commits.c.id).label('commit_count')

        ]).select_from(
            projects.join(
                work_items_sources, work_items_sources.c.project_id == projects.c.id
            ).join(
                work_items, work_items.c.work_items_source_id == work_items_sources.c.id
            ).join(
                work_items_commits, work_items_commits.c.work_item_id == work_items.c.id
            ).join(
                commits, work_items_commits.c.commit_id == commits.c.id
            )
        ).where(
            and_(
                projects.c.key == bindparam('key'),
                between(commits.c.commit_date, window.begin, window.end)
            )
        ).group_by(
            work_items.c.id
        )

    @staticmethod
    def sort_order(project_recently_active_work_items, **kwargs):
        return [project_recently_active_work_items.c.commit_count.desc()]


class ProjectRecentlyActiveRepositoriesNodes(ConnectionResolver):
    interfaces = (NamedNode, CommitCount)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        end_date = kwargs.get('before') or datetime.utcnow()
        window = time_window(begin=end_date - timedelta(days=kwargs.get('days', 7)), end=end_date)

        return select([
            repositories.c.id,
            func.min(cast(repositories.c.key, Text)).label('key'),
            func.min(repositories.c.name).label('name'),
            func.count(commits.c.id).label('commit_count')
        ]).select_from(
            projects.join(
                projects_repositories, projects_repositories.c.project_id == projects.c.id
            ).join(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            ).join(
                commits
            )
        ).where(
            and_(
                projects.c.key == bindparam('key'),
                projects_repositories.c.excluded == False,
                between(commits.c.commit_date, window.begin, window.end)
            )
        ).group_by(
            repositories.c.id
        )

    @staticmethod
    def sort_order(project_recently_active_repositories, **kwargs):
        return [project_recently_active_repositories.c.commit_count.desc()]


class ProjectCommitNodes(ConnectionResolver):
    interface = CommitInfo

    @staticmethod
    def connection_nodes_selector(**kwargs):
        select_project_commits = select([
            *commit_info_columns(repositories, commits, apply_distinct=True),
        ]).select_from(
            projects.join(
                work_items_sources, work_items_sources.c.project_id == projects.c.id
            ).join(
                work_items, work_items.c.work_items_source_id == work_items_sources.c.id
            ).join(
                work_items_commits, work_items_commits.c.work_item_id == work_items.c.id
            ).join(
                commits, work_items_commits.c.commit_id == commits.c.id
            ).join(
                repositories, commits.c.repository_id == repositories.c.id
            ).join(
                contributor_aliases, commits.c.author_contributor_alias_id == contributor_aliases.c.id
            )
        ).where(
            and_(
                projects.c.key == bindparam('key'),
                contributor_aliases.c.robot == False
            )
        )
        project_commits = commits_connection_apply_filters(select_project_commits, commits, **kwargs)

        select_untracked_commits = select([
            *commit_info_columns(repositories, commits)
        ]).select_from(
            projects.join(
                projects_repositories, projects_repositories.c.project_id == projects.c.id,
            ).join(
                repositories, projects_repositories.c.repository_id == repositories.c.id,
            ).join(
                commits, commits.c.repository_id == repositories.c.id
            ).join(
                contributor_aliases, commits.c.author_contributor_alias_id == contributor_aliases.c.id
            ).outerjoin(
                work_items_commits, work_items_commits.c.commit_id == commits.c.id
            )
        ).where(
            and_(
                projects.c.key == bindparam('key'),
                projects_repositories.c.excluded == False,
                work_items_commits.c.work_item_id == None,
                contributor_aliases.c.robot == False
            )
        )
        untracked_commits = commits_connection_apply_filters(select_untracked_commits, commits, **kwargs)

        if kwargs.get('nospecs_only'):
            return untracked_commits
        else:
            return union_all(
                project_commits,
                untracked_commits
            )

    @staticmethod
    def sort_order(project_commit_nodes, **kwargs):
        return [project_commit_nodes.c.commit_date.desc()]


class ProjectContributorNodes(ConnectionResolver):
    interface = NamedNode

    @staticmethod
    def connection_nodes_selector(**kwargs):
        select_stmt =  select([
            contributors.c.id,
            contributors.c.key,
            contributors.c.name,
            repositories_contributor_aliases.c.repository_id
        ]).select_from(
            contributors.join(
                repositories_contributor_aliases.join(
                    repositories.join(
                        projects_repositories
                    ).join(
                        projects
                    )
                )
            )
        ).where(
            and_(
                projects.c.key == bindparam('key'),
                repositories_contributor_aliases.c.robot == False,
                projects_repositories.c.excluded == False
            )
        ).distinct()
        return  contributors_connection_apply_filters(select_stmt, **kwargs)


class ProjectRecentlyActiveContributorNodes(ConnectionResolver):
    interfaces = (NamedNode, CommitCount)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        end_date = kwargs.get('before') or datetime.utcnow()
        window = time_window(begin=end_date - timedelta(days=kwargs.get('days', 7)), end=end_date)

        return select([
            commits.c.author_contributor_key.label('key'),
            func.min(commits.c.author_contributor_name).label('name'),
            func.count(distinct(commits.c.id)).label('commit_count')

        ]).select_from(
            projects.join(
                projects_repositories, projects_repositories.c.project_id == projects.c.id
            ).join(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            ).join(
                commits
            ).join(
                contributor_aliases, commits.c.author_contributor_alias_id == contributor_aliases.c.id
            )
        ).where(
            and_(
                projects.c.key == bindparam('key'),
                between(commits.c.commit_date, window.begin, window.end),
                contributor_aliases.c.robot == False,
                projects_repositories.c.excluded == False
            )
        ).group_by(
            commits.c.author_contributor_key
        )

    @staticmethod
    def sort_order(recently_active_contributors, **kwargs):
        return [recently_active_contributors.c.commit_count.desc()]


class ProjectWorkItemNodes(ConnectionResolver):
    interfaces = (NamedNode, WorkItemInfo, WorkItemsSourceRef)

    @classmethod
    def default_connection_nodes_selector(cls, work_items_connection_columns, **kwargs):
        select_stmt = select(
            work_items_connection_columns
        ).select_from(
            projects.join(
                work_items_sources, work_items_sources.c.project_id == projects.c.id
            ).join(
                work_items
            ).join(
                work_item_delivery_cycles,
                work_items.c.current_delivery_cycle_id == work_item_delivery_cycles.c.delivery_cycle_id
            )
        ).where(
            projects.c.key == bindparam('key')
        )
        select_stmt = work_items_connection_apply_filters(select_stmt, work_items, **kwargs)
        return work_item_delivery_cycles_connection_apply_filters(select_stmt, work_items, work_item_delivery_cycles,
                                                                  **kwargs)

    @classmethod
    def funnel_view_connection_nodes_selector(cls, work_items_connection_columns, **kwargs):
        project_nodes = select([
            projects.c.id,
            projects.c.name,
            projects.c.key
        ]).where(
            projects.c.key == bindparam('key')
        ).alias()

        select_work_items = select_funnel_work_items(project_nodes, work_items_connection_columns, **kwargs)
        return select([select_work_items])

    @classmethod
    def connection_nodes_selector(cls, **kwargs):
        work_items_connection_columns = [
            work_items.c.id,
            work_items.c.name,
            work_items.c.key,
            work_items_sources.c.key.label('work_items_source_key'),
            work_items_sources.c.name.label('work_items_source_name'),
            work_items_sources.c.integration_type.label('work_tracking_integration_type'),
            *work_item_info_columns(work_items),

        ]
        if 'funnel_view' in kwargs:
            return cls.funnel_view_connection_nodes_selector(work_items_connection_columns, **kwargs)
        else:
            return cls.default_connection_nodes_selector(work_items_connection_columns, **kwargs)

    @staticmethod
    def sort_order(project_work_items_nodes, **kwargs):
        return [project_work_items_nodes.c.updated_at.desc()]


class ProjectWorkItemEventNodes(ConnectionResolver):
    interfaces = (NamedNode, WorkItemInfo, WorkItemStateTransition, WorkItemsSourceRef)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        previous_state_type = work_items_source_state_map.alias()
        new_state_type = work_items_source_state_map.alias()

        select_stmt = select([
            work_items_sources.c.key.label('work_items_source_key'),
            work_items_sources.c.name.label('work_items_source_name'),
            work_items_sources.c.integration_type.label('work_tracking_integration_type'),
            *work_item_event_columns(work_items, work_item_state_transitions),
            previous_state_type.c.state_type.label('previous_state_type'),
            new_state_type.c.state_type.label('new_state_type')
        ]).select_from(
            projects.join(
                work_items_sources, work_items_sources.c.project_id == projects.c.id
            ).join(
                work_items
            ).join(
                work_item_state_transitions
            ).outerjoin(
                previous_state_type, and_(
                    previous_state_type.c.work_items_source_id == work_items_sources.c.id,
                    work_item_state_transitions.c.previous_state == previous_state_type.c.state
                )
            ).outerjoin(
                new_state_type, and_(
                    new_state_type.c.work_items_source_id == work_items_sources.c.id,
                    work_item_state_transitions.c.state == new_state_type.c.state
                )
            )
        ).where(
            projects.c.key == bindparam('key')
        )
        return work_item_events_connection_apply_time_window_filters(select_stmt, work_item_state_transitions, **kwargs)

    @staticmethod
    def sort_order(project_work_item_event_nodes, **kwargs):
        return [project_work_item_event_nodes.c.event_date.desc()]


class ProjectWorkItemCommitNodes(ConnectionResolver):
    interfaces = (NamedNode, WorkItemInfo, WorkItemCommitInfo, WorkItemsSourceRef)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        select_stmt = select([
            work_items_sources.c.key.label('work_items_source_key'),
            work_items_sources.c.name.label('work_items_source_name'),
            work_items_sources.c.integration_type.label('work_tracking_integration_type'),
            *work_item_info_columns(work_items),
            *work_item_commit_info_columns(work_items, repositories, commits)
        ]).select_from(
            projects.join(
                work_items_sources, work_items_sources.c.project_id == projects.c.id
            ).join(
                work_items
            ).join(
                work_items_commits
            ).join(
                commits
            ).join(
                repositories
            )
        ).where(
            projects.c.key == bindparam('key')
        )
        return commits_connection_apply_filters(select_stmt, commits, **kwargs)

    @staticmethod
    def sort_order(project_work_item_commits_nodes, **kwargs):
        return [project_work_item_commits_nodes.c.commit_date.desc()]


class ProjectWorkItemDeliveryCycleNodes(ConnectionResolver):
    interfaces = (NamedNode, WorkItemInfo, DeliveryCycleInfo, WorkItemsSourceRef)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        if kwargs.get('active_only'):
            delivery_cycles_join_clause = work_items.c.current_delivery_cycle_id == work_item_delivery_cycles.c.delivery_cycle_id
        else:
            delivery_cycles_join_clause = work_item_delivery_cycles.c.work_item_id == work_items.c.id

        select_stmt = select([
            work_item_delivery_cycles.c.delivery_cycle_id.label('id'),
            *work_item_delivery_cycle_info_columns(work_items, work_item_delivery_cycles),
            *work_item_info_columns(work_items),
            *work_items_source_ref_info_columns(work_items_sources)
        ]).select_from(
            projects.join(
                work_items_sources, work_items_sources.c.project_id == projects.c.id
            ).join(
                work_items, work_items.c.work_items_source_id == work_items_sources.c.id
            ).join(
                work_item_delivery_cycles, delivery_cycles_join_clause
            )
        ).where(
            projects.c.key == bindparam('key')
        )
        return work_item_delivery_cycles_connection_apply_filters(
            select_stmt, work_items, work_item_delivery_cycles, **kwargs
        )

    @staticmethod
    def sort_order(project_work_item_delivery_cycle_nodes, **kwargs):
        return [project_work_item_delivery_cycle_nodes.c.end_date.desc().nullsfirst()]


class ProjectCumulativeCommitCount(SelectableFieldResolver):
    interface = CumulativeCommitCount

    @staticmethod
    def selectable(**kwargs):
        commit_counts = select([
            extract('year', commits.c.commit_date).label('year'),
            extract('week', commits.c.commit_date).label('week'),
            func.count(commits.c.id).label('commit_count')
        ]).select_from(
            projects.join(
                projects_repositories, projects_repositories.c.project_id == projects.c.id
            ).join(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            ).join(
                commits, commits.c.repository_id == repositories.c.id
            )
        ).where(
            and_(
                projects.c.key == bindparam('key'),
                projects_repositories.c.excluded == False
            )
        ).group_by(
            extract('year', commits.c.commit_date),
            extract('week', commits.c.commit_date)
        ).alias('weekly_commit_counts')

        return select([
            commit_counts.c.year,
            commit_counts.c.week,
            func.sum(commit_counts.c.commit_count).over(order_by=[
                commit_counts.c.year,
                commit_counts.c.week
            ]).label('cumulative_commit_count')
        ])


class ProjectWeeklyContributorCount(SelectableFieldResolver):
    interface = WeeklyContributorCount

    @staticmethod
    def selectable(**kwargs):
        return select([
            extract('year', commits.c.commit_date).label('year'),
            extract('week', commits.c.commit_date).label('week'),
            func.count(distinct(commits.c.author_contributor_key)).label('contributor_count')
        ]).select_from(
            projects.join(
                projects_repositories, projects_repositories.c.project_id == projects.c.id
            ).join(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            ).join(
                commits, commits.c.repository_id == repositories.c.id
            )
        ).where(
            and_(
                projects.c.key == bindparam('key'),
                projects_repositories.c.excluded == False
            )
        ).group_by(
            extract('year', commits.c.commit_date),
            extract('week', commits.c.commit_date)
        )


class ProjectsProjectSetupInfo(InterfaceResolver):
    interface = ProjectSetupInfo

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        return select([
            project_nodes.c.id,
            func.count(work_items_sources.c.id.distinct()).label('work_stream_count'),
            func.count(work_items_source_state_map.c.work_items_source_id.distinct()).label('mapped_work_stream_count')

        ]).select_from(
            project_nodes.outerjoin(
                work_items_sources,
                work_items_sources.c.project_id == project_nodes.c.id
            ).outerjoin(
                work_items_source_state_map,
                and_(
                    work_items_source_state_map.c.work_items_source_id == work_items_sources.c.id,
                    work_items_source_state_map.c.state != 'created'
                )
            )
        ).group_by(project_nodes.c.id)


class ProjectsCommitSummary(InterfaceResolver):
    interface = CommitSummary

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        return select([
            project_nodes.c.id,
            func.sum(repositories.c.commit_count).label('commit_count'),
            func.min(repositories.c.earliest_commit).label('earliest_commit'),
            func.max(repositories.c.latest_commit).label('latest_commit')

        ]).select_from(
            project_nodes.outerjoin(
                projects_repositories, project_nodes.c.id == projects_repositories.c.project_id
            ).outerjoin(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            )
        ).where(
            projects_repositories.c.excluded == False
        ).group_by(project_nodes.c.id)

    @staticmethod
    def sort_order(projects_commit_summary, **kwargs):
        return [nulls_to_zero(projects_commit_summary.c.commit_count).desc()]


class ProjectsContributorCount(InterfaceResolver):
    interface = ContributorCount

    # A project contributor is someone who has authored code
    # for that project. If the contributor count days is provided
    # we filter for those contributors who have authored code that was
    # committed in given time period.

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        select_stmt = select([
            project_nodes.c.id,
            func.count(
                distinct(
                    contributor_aliases.c.contributor_id
                )
            ).label('contributor_count')

        ]).select_from(
            project_nodes.outerjoin(
                projects, project_nodes.c.id == projects.c.id
            ).join(
                projects_repositories, projects_repositories.c.project_id == projects.c.id
            ).join(
                work_items_sources, work_items_sources.c.project_id == project_nodes.c.id
            ).join(
                work_items, work_items.c.work_items_source_id == work_items_sources.c.id
            ).join(
                work_items_commits, work_items_commits.c.work_item_id == work_items.c.id
            ).join(
                commits,
                and_(
                    commits.c.repository_id == projects_repositories.c.repository_id,
                    work_items_commits.c.commit_id == commits.c.id
                )
            ).join(
                contributor_aliases, commits.c.author_contributor_alias_id == contributor_aliases.c.id
            )
        ).where(
            and_(
                contributor_aliases.c.robot == False,
                projects_repositories.c.excluded == False
            )
        )

        if 'contributor_count_days' in kwargs and kwargs['contributor_count_days'] > 0:
            commit_window_start = datetime.utcnow() - timedelta(days=kwargs['contributor_count_days'])
            select_stmt = select_stmt.where(
                commits.c.commit_date >= commit_window_start
            )

        return select_stmt.group_by(project_nodes.c.id)


class ProjectsDeliveryCycleSpan(InterfaceResolver):
    interface = DeliveryCycleSpan

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        select_stmt = select([
            project_nodes.c.id,
            func.min(work_item_delivery_cycles.c.end_date).label('earliest_closed_date'),
            func.max(work_item_delivery_cycles.c.end_date).label('latest_closed_date')
        ]).select_from(
            project_nodes.join(
                work_items_sources, work_items_sources.c.project_id == project_nodes.c.id
            ).join(
                work_items, work_items.c.work_items_source_id == work_items_sources.c.id
            ).join(
                work_item_delivery_cycles, work_item_delivery_cycles.c.work_item_id == work_items.c.id
            )
        )
        select_stmt = work_item_delivery_cycles_connection_apply_filters(
            select_stmt,
            work_items,
            work_item_delivery_cycles,
            **kwargs
        )

        return select_stmt.group_by(project_nodes.c.id)


class ProjectsRepositoryCount(InterfaceResolver):
    interface = RepositoryCount

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        return select([
            project_nodes.c.id,
            func.count(repositories.c.id).label('repository_count')

        ]).select_from(
            project_nodes.outerjoin(
                projects_repositories, project_nodes.c.id == projects_repositories.c.project_id
            ).outerjoin(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            )
        ).where(
            projects_repositories.c.excluded == False
        ).group_by(project_nodes.c.id)


class ProjectsOrganizationRef(InterfaceResolver):
    interface = OrganizationRef

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        return select([
            project_nodes.c.id,
            organizations.c.key.label('organization_key'),
            organizations.c.name.label('organization_name')

        ]).select_from(
            project_nodes.outerjoin(
                projects, project_nodes.c.id == projects.c.id
            ).outerjoin(
                organizations, projects.c.organization_id == organizations.c.id
            )
        )


class ProjectsArchivedStatus(InterfaceResolver):
    interface = ArchivedStatus

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        return select([
            project_nodes.c.id,
            projects.c.archived
        ]).select_from(
            project_nodes.outerjoin(
                projects, project_nodes.c.id == projects.c.id
            )
        )


class ProjectWorkItemEventSpan(InterfaceResolver):
    interface = WorkItemEventSpan

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        return select([
            project_nodes.c.id,
            func.min(work_items.c.created_at).label('earliest_work_item_event'),
            func.max(work_items.c.updated_at).label('latest_work_item_event')
        ]).select_from(
            project_nodes.outerjoin(
                work_items_sources, work_items_sources.c.project_id == project_nodes.c.id
            ).outerjoin(
                work_items, work_items.c.work_items_source_id == work_items_sources.c.id
            )
        ).group_by(project_nodes.c.id)


class ProjectPullRequestEventSpan(InterfaceResolver):
    interface = PullRequestEventSpan

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        return select([
            project_nodes.c.id,
            func.max(pull_requests.c.updated_at).label('latest_pull_request_event')
        ]).select_from(
            project_nodes.join(
                projects_repositories, projects_repositories.c.project_id == project_nodes.c.id
            ).join(
                pull_requests, pull_requests.c.repository_id == projects_repositories.c.repository_id
            )
        ).where(
            projects_repositories.c.excluded == False
        ).group_by(project_nodes.c.id)


class ProjectFunnelViewAggregateMetrics(InterfaceResolver):
    interface = FunnelViewAggregateMetrics

    @classmethod
    def interface_selector(cls, project_nodes, **kwargs):
        shared_work_items_columns = [
            project_nodes.c.id.label('project_id'),
            work_item_delivery_cycles.c.delivery_cycle_id,
            work_item_delivery_cycles.c.effort,
            work_item_delivery_cycles.c.end_date,
            work_item_delivery_cycles.c.commit_count,
            work_items.c.is_bug,
        ]

        selected_work_items = select_funnel_work_items(project_nodes, shared_work_items_columns, **kwargs)

        # aggregate metrics by project_id and state_type
        work_items_by_state_type = select([
            selected_work_items.c.project_id.label('id'),
            selected_work_items.c.state_type,
            func.count(selected_work_items.c.delivery_cycle_id).label('count'),
            func.sum(selected_work_items.c.effort).label('total_effort'),
        ]).select_from(
            selected_work_items
        ).group_by(
            selected_work_items.c.project_id,
            selected_work_items.c.state_type
        ).alias()

        return select([
            project_nodes.c.id,
            func.json_agg(
                case([
                    (
                        work_items_by_state_type.c.id != None,
                        func.json_build_object(
                            'state_type', work_items_by_state_type.c.state_type,
                            'count', work_items_by_state_type.c.count
                        )
                    )
                ], else_=None)
            ).label('work_item_state_type_counts'),
            func.json_agg(
                case([
                    (
                        work_items_by_state_type.c.id != None,
                        func.json_build_object(
                            'state_type', work_items_by_state_type.c.state_type,
                            'total_effort', func.coalesce(work_items_by_state_type.c.total_effort, 0)
                        )
                    )
                ], else_=None)
            ).label('total_effort_by_state_type')

        ]).select_from(
            project_nodes.outerjoin(
                work_items_by_state_type, project_nodes.c.id == work_items_by_state_type.c.id
            )
        ).group_by(
            project_nodes.c.id
        )


class ProjectCycleMetrics(InterfaceResolver):
    interface = AggregateCycleMetrics

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        target_percentile = kwargs.get('cycle_metrics_target_percentile')
        work_items_cycle_metrics = sql_expressions.work_items_cycle_metrics(
            **dict_merge(
                kwargs,
                dict(work_item_types=WorkItemTypesToIncludeInCycleMetrics)
            )
        ).alias()

        project_work_item_cycle_metrics = select([
            project_nodes.c.id.label('project_id'),
            work_items_cycle_metrics.c.id.label('work_item_id'),
            work_items_cycle_metrics.c.lead_time,
            work_items_cycle_metrics.c.cycle_time,
            work_items_cycle_metrics.c.commit_count,
            work_items_cycle_metrics.c.end_date,
        ]).select_from(
            project_nodes.join(
                work_items_sources, work_items_sources.c.project_id == project_nodes.c.id
            ).join(
                work_items_cycle_metrics, work_items_cycle_metrics.c.work_items_source_id == work_items_sources.c.id
            )).alias()

        return select([
            project_nodes.c.id,
            literal(datetime.utcnow()).label('measurement_date'),
            literal(kwargs.get('closed_within_days')).label('measurement_window'),
            func.min(project_work_item_cycle_metrics.c.lead_time).label('min_lead_time'),
            func.avg(project_work_item_cycle_metrics.c.lead_time).label('avg_lead_time'),
            func.max(project_work_item_cycle_metrics.c.lead_time).label('max_lead_time'),
            func.min(project_work_item_cycle_metrics.c.cycle_time).label('min_cycle_time'),
            func.avg(project_work_item_cycle_metrics.c.cycle_time).label('avg_cycle_time'),
            func.max(project_work_item_cycle_metrics.c.cycle_time).label('max_cycle_time'),
            func.percentile_disc(target_percentile).within_group(project_work_item_cycle_metrics.c.lead_time).label(
                'percentile_lead_time'),
            func.percentile_disc(target_percentile).within_group(project_work_item_cycle_metrics.c.cycle_time).label(
                'percentile_cycle_time'),
            func.min(project_work_item_cycle_metrics.c.end_date).label('earliest_closed_date'),
            func.max(project_work_item_cycle_metrics.c.end_date).label('latest_closed_date'),
            func.count(project_work_item_cycle_metrics.c.work_item_id.distinct()).label('work_items_in_scope'),
            func.sum(
                case([
                    (and_(project_work_item_cycle_metrics.c.work_item_id != None,
                          project_work_item_cycle_metrics.c.cycle_time == None), 1)
                ], else_=0)
            ).label('work_items_with_null_cycle_time'),
            func.sum(
                case([
                    (
                        and_(
                            project_work_item_cycle_metrics.c.work_item_id != None,
                            project_work_item_cycle_metrics.c.commit_count > 0
                        ), 1
                    )
                ], else_=0
                )
            ).label('work_items_with_commits'),
            literal_column(f'{target_percentile}').label('target_percentile'),
            literal_column(f'{target_percentile}').label('cycle_time_target_percentile'),
            literal_column(f'{target_percentile}').label('lead_time_target_percentile')
        ]).select_from(
            project_nodes.outerjoin(
                project_work_item_cycle_metrics, project_work_item_cycle_metrics.c.project_id == project_nodes.c.id
            )
        ).group_by(
            project_nodes.c.id
        )


class ProjectCycleMetricsTrends(CycleMetricsTrendsBase):
    interface = CycleMetricsTrends

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        cycle_metrics_trends_args = kwargs.get('cycle_metrics_trends_args')

        # Get the a list of dates for trending using the trends_args for control
        timeline_dates = get_timeline_dates_for_trending(
            cycle_metrics_trends_args,
            arg_name='cycle_metrics_trends',
            interface_name='CycleMetricTrends'
        )

        project_nodes_dates = select([project_nodes, timeline_dates]).cte()

        measurement_window = cycle_metrics_trends_args.measurement_window
        if measurement_window is None:
            raise ProcessingException(
                "'measurement_window' must be specified when calculating ProjectCycleMetricsTrends"
            )

        metrics_map = ProjectCycleMetricsTrends.get_metrics_map(
            cycle_metrics_trends_args,
            delivery_cycles_relation=work_item_delivery_cycles
        )
        # Now for each of these dates, we are going to be aggregating the measurements for work items
        # within the measurement window for that date. We will be using a *lateral* join for doing the full aggregation
        # so note the lateral clause at the end instead of the usual alias.
        cycle_metrics = select([
            project_nodes_dates.c.id.label('project_id'),
            project_nodes_dates.c.measurement_date,
            # These are standard attributes returned for the the AggregateCycleMetricsInterface

            func.max(work_item_delivery_cycles.c.end_date).label('latest_closed_date'),
            func.min(work_item_delivery_cycles.c.end_date).label('earliest_closed_date'),
            *[
                # This interpolates columns that calculate the specific cycle
                # metrics that need to be returned based on the metrics specified in the cycle_metrics_trends_args
                *ProjectCycleMetricsTrends.get_metrics_columns(
                    cycle_metrics_trends_args, metrics_map, 'cycle_metrics'
                ),

                # This interpolates columns that calculate the specific work item count related
                # metrics that need to be returned based on the metrics specified in the cycle_metrics_trends_args
                *ProjectCycleMetricsTrends.get_metrics_columns(
                    cycle_metrics_trends_args, metrics_map, 'work_item_counts'
                ),

                # This interpolates columns that calculate the specific implementation_complexity
                # metrics that need to be returned based on the metrics specified in the cycle_metrics_trends_args
                *ProjectCycleMetricsTrends.get_metrics_columns(
                    cycle_metrics_trends_args, metrics_map, 'implementation_complexity'
                )
            ]

        ]).select_from(
            project_nodes_dates.join(
                work_items_sources, work_items_sources.c.project_id == project_nodes_dates.c.id
            ).join(
                work_items,
                and_(
                    work_items.c.work_items_source_id == work_items_sources.c.id,
                    *ProjectCycleMetricsTrends.get_work_item_filter_clauses(cycle_metrics_trends_args, kwargs)
                )
            ).outerjoin(
                # outer join here because we want to report timelines dates even
                # when there are no work items closed in that period.
                work_item_delivery_cycles,
                and_(
                    work_item_delivery_cycles.c.work_item_id == work_items.c.id,
                    # The logic here is as follows:
                    # It measurement date is d, then we will include evey delivery
                    # cycle that closed on the date d which is why the end date is d + 1,
                    # and window-1 days prior. So if window = 1 we will only include the
                    # delivery cycles that closed on the measurement_date.
                    date_column_is_in_measurement_window(
                        work_item_delivery_cycles.c.end_date,
                        measurement_date=project_nodes_dates.c.measurement_date,
                        measurement_window=measurement_window
                    ),
                    *ProjectCycleMetricsTrends.get_work_item_delivery_cycle_filter_clauses(
                        cycle_metrics_trends_args
                    )
                )
            )
        ).group_by(
            project_nodes_dates.c.id,
            project_nodes_dates.c.measurement_date
        ).order_by(
            project_nodes_dates.c.id,
            project_nodes_dates.c.measurement_date.desc()
        ).alias()

        return select([
            cycle_metrics.c.project_id.label('id'),
            func.json_agg(
                func.json_build_object(
                    'measurement_date', cycle_metrics.c.measurement_date,
                    'measurement_window', measurement_window,
                    'earliest_closed_date', cycle_metrics.c.earliest_closed_date,
                    'latest_closed_date', cycle_metrics.c.latest_closed_date,
                    'lead_time_target_percentile', cycle_metrics_trends_args.lead_time_target_percentile,
                    'cycle_time_target_percentile', cycle_metrics_trends_args.cycle_time_target_percentile,
                    *[
                        *ProjectCycleMetricsTrends.get_cycle_metrics_json_object_columns(
                            cycle_metrics_trends_args, metrics_map, cycle_metrics
                        ),
                        *ProjectCycleMetricsTrends.get_work_item_count_metrics_json_object_columns(
                            cycle_metrics_trends_args, metrics_map, cycle_metrics
                        ),
                        *ProjectCycleMetricsTrends.get_implementation_complexity_metrics_json_object_columns(
                            cycle_metrics_trends_args, metrics_map, cycle_metrics
                        )
                    ],
                )

            ).label('cycle_metrics_trends')
        ]).select_from(
            cycle_metrics
        ).group_by(
            cycle_metrics.c.project_id
        )


class ProjectPipelineCycleMetrics(CycleMetricsTrendsBase):
    interface = PipelineCycleMetrics

    @classmethod
    def get_delivery_cycle_relation_for_pipeline(cls, kwargs, cycle_metrics_trends_args, measurement_date, project_nodes):
        # This query provides a relation with a column interface similar to
        # work_item_delivery_cycles, but with cycle time and lead time calculated dynamically
        # for work items in the current pipeline. Since cycle_time and lead_time are cached once
        # and for all only on the work items that are closed, we need to calculate these
        # dynamically here. Modulo this, much of the cycle time trending logic is similar across
        # open and closed items, so this lets us share all that logic across both cases.
        return select([
            project_nodes.c.id,
            work_items.c.id.label('work_item_id'),
            # we need these columns to satisfy the delivery_cycle_relation contract for computing metrics
            func.min(work_item_delivery_cycles.c.delivery_cycle_id).label('delivery_cycle_id'),
            func.min(work_item_delivery_cycles.c.end_date).label('end_date'),
            func.min(work_item_delivery_cycles.c.commit_count).label('commit_count'),
            func.min(work_item_delivery_cycles.c.effort).label('effort'),
            func.min(work_item_delivery_cycles.c.earliest_commit).label('earliest_commit'),
            func.max(work_item_delivery_cycles.c.latest_commit).label('latest_commit'),
            # the time from the start of the delivery cycle to the measurement date is the elapsed lead time for this
            # work item
            func.extract('epoch', measurement_date - func.min(work_item_delivery_cycles.c.start_date)).label(
                'lead_time'),

            # cycle time = lead_time - backlog time.
            (
                    func.extract('epoch', measurement_date - func.min(work_item_delivery_cycles.c.start_date)) -
                    # This is calculated backlog time
                    # there can be multiple backlog states, so we could have a duration in each one,
                    # so taking the sum correctly gets you the current backlog duration.
                    func.sum(
                        case(
                            [
                                (work_items_source_state_map.c.state_type == WorkItemsStateType.backlog.value,
                                 work_item_delivery_cycle_durations.c.cumulative_time_in_state)
                            ],
                            else_=0
                        )
                    )
            ).label(
                # using spec_cycle_time here purely to coerce the column name so that
                # we can use the same logic from the base class. The coupling between
                # this method and the base class method needs to be revisited since
                # the logic for the closed and pipeline metrics have now diverged significantly
                # that it may make sense to treat them separately.
                'spec_cycle_time'
            ),
            # Latency for in-progress items = measurement_date - latest_commit
            (
                func.extract(
                    'epoch',
                    measurement_date - func.coalesce(
                        func.min(work_item_delivery_cycles.c.latest_commit),
                        # if latest_commit is null, then its is not a spec - so latency is 0
                        measurement_date
                    )
                )
            ).label('latency')

        ]).select_from(
            # get current delivery cycles for all the work items in the pipeline
            project_nodes.join(
                work_items_sources, work_items_sources.c.project_id == project_nodes.c.id
            ).join(
                work_items,
                work_items.c.work_items_source_id == work_items_sources.c.id
            ).join(
                work_items_source_state_map,
                work_items_source_state_map.c.work_items_source_id == work_items_sources.c.id
            ).join(
                work_item_delivery_cycles,
                work_item_delivery_cycles.c.delivery_cycle_id == work_items.c.current_delivery_cycle_id
            ).join(
                # along with the durations of all states (could be multiple)
                work_item_delivery_cycle_durations,
                and_(
                    work_item_delivery_cycle_durations.c.delivery_cycle_id == work_item_delivery_cycles.c.delivery_cycle_id,
                    work_item_delivery_cycle_durations.c.state == work_items_source_state_map.c.state
                )
            )
        ).where(
            and_(
                # we include work items that are not in backlog or closed: this the filter for active items only
                work_items.c.state_type.notin_([WorkItemsStateType.backlog.value, WorkItemsStateType.closed.value]),
                # filter out the work items state durations that are in backlog state (could be multiple)
                # This gives the data to calculate the time in backlog so that we can subtract this from
                # lead time to get cycle time
                # work_items_source_state_map.c.state_type == WorkItemsStateType.backlog.value,
                # add any other work item filters that the caller specifies.
                *ProjectCycleMetricsTrends.get_work_item_filter_clauses(cycle_metrics_trends_args, kwargs),
                # add delivery cycle related filters that the caller specifies
                *ProjectCycleMetricsTrends.get_work_item_delivery_cycle_filter_clauses(
                    cycle_metrics_trends_args
                )
            )
        ).group_by(
            project_nodes.c.id,
            work_items.c.id,
        ).alias()

    @classmethod
    def interface_selector(cls, project_nodes, **kwargs):
        cycle_metrics_trends_args = kwargs.get('pipeline_cycle_metrics_args')

        measurement_date = datetime.utcnow()

        cycle_times = cls.get_delivery_cycle_relation_for_pipeline(
            kwargs, cycle_metrics_trends_args, measurement_date, project_nodes)

        metrics_map = ProjectCycleMetricsTrends.get_metrics_map(
            cycle_metrics_trends_args,
            delivery_cycles_relation=cycle_times
        )

        # Calculate the cycle metrics
        cycle_metrics = select([
            project_nodes.c.id.label('project_id'),
            *[
                # This interpolates columns that calculate the specific cycle
                # metrics that need to be returned based on the metrics specified in the cycle_metrics_trends_args
                *cls.get_metrics_columns(cycle_metrics_trends_args, metrics_map, 'cycle_metrics'),

                # This interpolates columns that calculate the specific work item count
                # metrics that need to be returned based on the metrics specified in the cycle_metrics_trends_args
                *cls.get_metrics_columns(cycle_metrics_trends_args, metrics_map, 'work_item_counts'),

                # This interpolates columns that calculate the specific implementation complexity
                # metrics that need to be returned based on the metrics specified in the cycle_metrics_trends_args
                *cls.get_metrics_columns(cycle_metrics_trends_args, metrics_map, 'implementation_complexity'),
            ]

        ]).select_from(
            project_nodes.outerjoin(
                cycle_times, project_nodes.c.id == cycle_times.c.id
            )
        ).group_by(
            project_nodes.c.id
        ).alias()

        # Serialize metrics into json columns for returning.
        return select([
            cycle_metrics.c.project_id.label('id'),
            func.json_agg(
                func.json_build_object(
                    'measurement_date', cast(measurement_date, Date),
                    'lead_time_target_percentile', cycle_metrics_trends_args.lead_time_target_percentile,
                    'cycle_time_target_percentile', cycle_metrics_trends_args.cycle_time_target_percentile,
                    *[
                        *ProjectCycleMetricsTrends.get_cycle_metrics_json_object_columns(
                            cycle_metrics_trends_args, metrics_map, cycle_metrics
                        ),
                        *ProjectCycleMetricsTrends.get_work_item_count_metrics_json_object_columns(
                            cycle_metrics_trends_args, metrics_map, cycle_metrics
                        ),
                        *ProjectCycleMetricsTrends.get_implementation_complexity_metrics_json_object_columns(
                            cycle_metrics_trends_args, metrics_map, cycle_metrics
                        )
                    ],
                )

            ).label('pipeline_cycle_metrics')
        ]).select_from(
            cycle_metrics
        ).group_by(
            cycle_metrics.c.project_id
        )


class ProjectTraceabilityTrends(InterfaceResolver):
    interface = TraceabilityTrends

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        traceability_trends_args = kwargs.get('traceability_trends_args')
        measurement_window = traceability_trends_args.measurement_window

        if measurement_window is None:
            raise ProcessingException(
                "'measurement_window' must be specified when calculating ProjectCycleMetricsTrends"
            )

        # Get a list of dates for trending using the trends_args for control
        timeline_dates = get_timeline_dates_for_trending(
            traceability_trends_args,
            arg_name='traceability_trends',
            interface_name='TraceabilityTrends'
        )

        # compute the overall span of dates in the trending window so we can scan and load all the
        # relevant commits that fall within that window. This is more efficient than scanning
        # all the repositories and then the commits for each measurement point.
        timeline_span = select([
            (func.min(timeline_dates.c.measurement_date) - timedelta(days=measurement_window)).label('window_start'),
            func.max(timeline_dates.c.measurement_date).label('window_end')
        ]).cte()
        # find the candidate commits. These are all the commits that belong
        # to all repositories associated with the project, which fall within the
        # timeline span
        candidate_commits = select([
            project_nodes.c.id,
            projects_repositories.c.repository_id,
            commits.c.id.label('commit_id'),
            commits.c.commit_date
        ]).select_from(
            project_nodes.join(
                projects_repositories, projects_repositories.c.project_id == project_nodes.c.id
            ).join(
                commits, commits.c.repository_id == projects_repositories.c.repository_id
            ).join(
                contributor_aliases, commits.c.author_contributor_alias_id == contributor_aliases.c.id
            )
        ).where(
            and_(
                projects_repositories.c.excluded == False,
                contributor_aliases.c.robot == False,
                commits.c.commit_date.between(
                    timeline_span.c.window_start,
                    timeline_span.c.window_end
                )
            )
        )
        if traceability_trends_args.exclude_merges:
            candidate_commits = candidate_commits.where(
                commits.c.num_parents <= 1
            )

        candidate_commits = candidate_commits.cte()

        # do the cross join to compute one row for each project-repo combination and each trend measurement date
        # we will compute the traceability metrics for each of these rows
        projects_timeline_dates = select([project_nodes, timeline_dates]).alias()

        # we compute the total commits and spec counts for
        # each measurement date and repository combination.
        # for each of these points we are aggregating the candidate commits
        # that fall within the window to count the commits and specs
        traceability_metrics_base = select([
            projects_timeline_dates.c.id,
            projects_timeline_dates.c.measurement_date,
            # The total commits include all the candidate commits in the specific
            # measurement window.
            func.count(candidate_commits.c.commit_id.distinct()).label('total_commits'),
            # The specs are the subset of those commits that are associated with this
            # specific project. Note that under this definition, if a repo is shared across
            # multiple projects, the traceability metrics for any one of the projects will generally
            # be low even if the overall traceability of the repo itself is high. This is why
            # we need to provide the ability to fine tune this traceability to only include those
            # repos where "most" of the work is done for a single project. Traceability for projects
            # is different from traceability for repositories in this sense.
            func.count(candidate_commits.c.commit_id.distinct()).filter(
                work_items_sources.c.project_id == projects_timeline_dates.c.id
            ).label('spec_count')
        ]).select_from(
            projects_timeline_dates.join(
                candidate_commits,
                candidate_commits.c.id == projects_timeline_dates.c.id
            ).outerjoin(
                work_items_commits,
                work_items_commits.c.commit_id == candidate_commits.c.commit_id
            ).outerjoin(
                work_items, work_items_commits.c.work_item_id == work_items.c.id
            ).outerjoin(
                work_items_sources, work_items.c.work_items_source_id == work_items_sources.c.id
            )
        ).where(
            date_column_is_in_measurement_window(
                candidate_commits.c.commit_date,
                measurement_date=projects_timeline_dates.c.measurement_date,
                measurement_window=measurement_window
            )
        ).group_by(
            projects_timeline_dates.c.id,
            projects_timeline_dates.c.measurement_date
        ).alias()
        # outer join with the timeline dates to make sure we get one entry per repository and date in the
        # series and that the series is ordered by descending date.
        traceability_metrics = select([
            projects_timeline_dates.c.id,
            projects_timeline_dates.c.measurement_date,
            traceability_metrics_base.c.total_commits,
            traceability_metrics_base.c.spec_count
        ]).select_from(
            projects_timeline_dates.outerjoin(
                traceability_metrics_base,
                and_(
                    projects_timeline_dates.c.id == traceability_metrics_base.c.id,
                    projects_timeline_dates.c.measurement_date == traceability_metrics_base.c.measurement_date
                )
            )
        ).order_by(
            projects_timeline_dates.c.id,
            projects_timeline_dates.c.measurement_date.desc()
        ).alias()
        return select([
            traceability_metrics.c.id,
            func.json_agg(
                func.json_build_object(
                    'measurement_date', cast(traceability_metrics.c.measurement_date, Date),
                    'measurement_window', measurement_window,
                    'total_commits', func.coalesce(traceability_metrics.c.total_commits, 0),
                    'spec_count', func.coalesce(traceability_metrics.c.spec_count, 0),
                    'nospec_count',
                    func.coalesce(traceability_metrics.c.total_commits - traceability_metrics.c.spec_count, 0),
                    'traceability', case([
                        (traceability_metrics.c.total_commits > 0,
                         (traceability_metrics.c.spec_count / (traceability_metrics.c.total_commits * 1.0)))
                    ], else_=0
                    )
                )
            ).label('traceability_trends')
        ]).select_from(
            traceability_metrics
        ).group_by(
            traceability_metrics.c.id
        )


class ProjectResponseTimeConfidenceTrends(InterfaceResolver):
    interface = ResponseTimeConfidenceTrends

    @classmethod
    def response_time_rank_query(cls, projects_timeline_dates, measurement_window, metric, target_value, **kwargs):
        response_time_confidence_trends_args = kwargs.get('response_time_confidence_trends_args')

        response_times = select([
            projects_timeline_dates.c.id,
            projects_timeline_dates.c.measurement_date,
            work_item_delivery_cycles.c.delivery_cycle_id,
            work_item_delivery_cycles.c[metric]
        ]).select_from(
            projects_timeline_dates.join(
                work_items_sources,
                work_items_sources.c.project_id == projects_timeline_dates.c.id
            ).join(
                work_items, work_items.c.work_items_source_id == work_items_sources.c.id
            ).join(
                work_item_delivery_cycles,
                work_item_delivery_cycles.c.work_item_id == work_items.c.id
            )
        ).where(
            and_(
                work_item_delivery_cycles.c[metric] != None,
                date_column_is_in_measurement_window(
                    work_item_delivery_cycles.c.end_date,
                    measurement_date=projects_timeline_dates.c.measurement_date,
                    measurement_window=measurement_window
                ),
                *ProjectCycleMetricsTrends.get_work_item_filter_clauses(response_time_confidence_trends_args, kwargs),
                *ProjectCycleMetricsTrends.get_work_item_delivery_cycle_filter_clauses(
                    response_time_confidence_trends_args
                )
            )
        ).alias()

        response_time_ranks = select([
            response_times.c.id,
            response_times.c.measurement_date,
            response_times.c[metric],
            response_times.c.delivery_cycle_id,
            func.cume_dist().over(
                order_by=[response_times.c[metric]],
                partition_by=[response_times.c.id, response_times.c.measurement_date]
            ).label('rank')
        ]).alias()

        query = select([
            response_time_ranks.c.id,
            response_time_ranks.c.measurement_date,
            func.max(response_time_ranks.c.rank).label('rank')
        ]).where(
            response_time_ranks.c[metric] <= target_value
        ).group_by(
            response_time_ranks.c.id,
            response_time_ranks.c.measurement_date
        )
        return query

    @classmethod
    def interface_selector(cls, project_nodes, **kwargs):
        response_time_confidence_trends_args = kwargs.get('response_time_confidence_trends_args')

        measurement_window = response_time_confidence_trends_args.measurement_window
        if measurement_window is None:
            raise ProcessingException(
                "'measurement_window' must be specified when calculating ProjectResponseTimeConfidenceTrends"
            )

        # Get the a list of dates for trending using the trends_args for control
        timeline_dates = get_timeline_dates_for_trending(
            response_time_confidence_trends_args,
            arg_name='response_time_confidence_trends',
            interface_name='ResponseTimeConfidenceTrends'
        )

        projects_timeline_dates = select([project_nodes.c.id, timeline_dates]).cte()

        lead_time_rank = cls.response_time_rank_query(
            projects_timeline_dates,
            measurement_window,
            metric='lead_time',
            target_value=response_time_confidence_trends_args.lead_time_target * 24 * 3600,
            **kwargs
        ).cte()

        cycle_time_rank = cls.response_time_rank_query(
            projects_timeline_dates,
            measurement_window,
            metric='cycle_time',
            target_value=response_time_confidence_trends_args.cycle_time_target * 24 * 3600,
            **kwargs
        ).cte()

        return select([
            projects_timeline_dates.c.id,
            func.json_agg(
                func.json_build_object(
                    'measurement_date', func.cast(projects_timeline_dates.c.measurement_date, Date),
                    'measurement_window', response_time_confidence_trends_args.measurement_window,
                    'lead_time_target', response_time_confidence_trends_args.lead_time_target,
                    'lead_time_confidence', func.coalesce(lead_time_rank.c.rank, 0).label('lead_time_confidence'),
                    'cycle_time_target', response_time_confidence_trends_args.cycle_time_target,
                    'cycle_time_confidence', func.coalesce(cycle_time_rank.c.rank, 0).label('cycle_time_confidence')
                )
            ).label('response_time_confidence_trends')
        ]).select_from(
            projects_timeline_dates.outerjoin(
                lead_time_rank, and_(
                    projects_timeline_dates.c.id == lead_time_rank.c.id,
                    projects_timeline_dates.c.measurement_date == lead_time_rank.c.measurement_date
                )
            ).outerjoin(
                cycle_time_rank,
                and_(
                    projects_timeline_dates.c.id == cycle_time_rank.c.id,
                    projects_timeline_dates.c.measurement_date == cycle_time_rank.c.measurement_date
                )
            )
        ).group_by(
            projects_timeline_dates.c.id,
        )


class ProjectsFlowMixTrends(InterfaceResolver):
    interface = FlowMixTrends

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        flow_mix_trends_args = kwargs.get('flow_mix_trends_args')

        measurement_window = flow_mix_trends_args.measurement_window
        if measurement_window is None:
            raise ProcessingException(
                "'measurement_window' must be specified when calculating FlowMixTrends"
            )

        # Get the a list of dates for trending using the trends_args for control
        timeline_dates = get_timeline_dates_for_trending(
            flow_mix_trends_args,
            arg_name='flow_mix_trends',
            interface_name='FlowMixTrends'
        )

        projects_timeline_dates = select([project_nodes.c.id, timeline_dates]).cte()

        select_work_items = select([
            projects_timeline_dates.c.id,
            projects_timeline_dates.c.measurement_date,
            work_items.c.id.label('work_item_id'),
            work_items.c.work_item_type,
            map_work_item_type_to_flow_type(work_items).label('category'),
            work_item_delivery_cycles.c.effort.label('effort')
        ]).select_from(
            projects_timeline_dates.join(
                work_items_sources, work_items_sources.c.project_id == projects_timeline_dates.c.id
            ).join(
                work_items, work_items.c.work_items_source_id == work_items_sources.c.id
            ).join(
                work_item_delivery_cycles, work_item_delivery_cycles.c.work_item_id == work_items.c.id
            )
        ).where(
            and_(
                date_column_is_in_measurement_window(
                    work_item_delivery_cycles.c.end_date,
                    measurement_date=projects_timeline_dates.c.measurement_date,
                    measurement_window=measurement_window
                ),
                *ProjectCycleMetricsTrends.get_work_item_filter_clauses(flow_mix_trends_args, kwargs),
                *ProjectCycleMetricsTrends.get_work_item_delivery_cycle_filter_clauses(
                    flow_mix_trends_args
                )
            )
        ).cte()

        select_category_counts = select([
            projects_timeline_dates.c.id,
            projects_timeline_dates.c.measurement_date,
            select_work_items.c.category,
            func.count(select_work_items.c.work_item_id.distinct()).label('work_item_count'),
            func.sum(select_work_items.c.effort).label('total_effort')
        ]).select_from(
            projects_timeline_dates.outerjoin(
                select_work_items,
                and_(
                    projects_timeline_dates.c.id == select_work_items.c.id,
                    projects_timeline_dates.c.measurement_date == select_work_items.c.measurement_date
                )
            )
        ).group_by(
            projects_timeline_dates.c.id,
            projects_timeline_dates.c.measurement_date,
            select_work_items.c.category
        ).alias()

        select_flow_mix = select([
            select_category_counts.c.id,
            select_category_counts.c.measurement_date,
            func.json_agg(
                func.json_build_object(
                    'category',
                    select_category_counts.c.category,
                    'work_item_count',
                    select_category_counts.c.work_item_count,
                    'total_effort',
                    select_category_counts.c.total_effort,
                )
            ).label('flow_mix')
        ]).select_from(
            select_category_counts
        ).group_by(
            select_category_counts.c.id,
            select_category_counts.c.measurement_date
        ).order_by(
            select_category_counts.c.id,
            select_category_counts.c.measurement_date.desc(),
        ).alias()

        return select([
            select_flow_mix.c.id,
            func.json_agg(
                func.json_build_object(
                    'measurement_date', select_flow_mix.c.measurement_date,
                    'measurement_window', measurement_window,
                    'flow_mix', select_flow_mix.c.flow_mix
                )
            ).label('flow_mix_trends')
        ]).select_from(
            select_flow_mix
        ).group_by(
            select_flow_mix.c.id
        )


class ProjectsCapacityTrends(InterfaceResolver):
    interface = CapacityTrends

    @classmethod
    def get_aggregate_capacity_trends(cls, measurement_window, project_nodes, timeline_dates):

        select_capacity = select([
            # Note - See related comment in get_contributor_level_capacity_trends
            # we are doing the cross join to projects here deliberately.
            projects.c.id,
            timeline_dates.c.measurement_date,
            commits.c.author_contributor_key,
            func.count(commit_day(commits).distinct()).label('commit_days')
        ]).select_from(
            timeline_dates.join(
                projects, true()
            ).join(
                projects_repositories, projects_repositories.c.project_id == projects.c.id
            ).join(
                work_items_sources, work_items_sources.c.project_id == projects.c.id
            ).join(
                work_items, work_items.c.work_items_source_id == work_items_sources.c.id
            ).join(
                work_items_commits, work_items_commits.c.work_item_id == work_items.c.id
            ).join(
                commits,
                and_(
                    work_items_commits.c.commit_id == commits.c.id,
                    projects_repositories.c.repository_id == commits.c.repository_id
                )
            ).join(
                contributor_aliases, commits.c.author_contributor_alias_id == contributor_aliases.c.id
            )
        ).where(
            and_(
                date_column_is_in_measurement_window(
                    commits.c.commit_date,
                    measurement_date=timeline_dates.c.measurement_date,
                    measurement_window=measurement_window
                ),
                contributor_aliases.c.robot == False,
                projects_repositories.c.excluded == False,
                projects.c.id.in_(select([project_nodes.c.id]))
            )
        ).group_by(
            projects.c.id,
            timeline_dates.c.measurement_date,
            commits.c.author_contributor_key
        ).alias()

        capacity_metrics = select([
            select_capacity.c.id,
            select_capacity.c.measurement_date,
            func.sum(select_capacity.c.commit_days).label('total_commit_days'),
            func.avg(select_capacity.c.commit_days).label('avg_commit_days'),
            func.min(select_capacity.c.commit_days).label('min_commit_days'),
            func.max(select_capacity.c.commit_days).label('max_commit_days'),
            func.count(select_capacity.c.author_contributor_key.distinct()).label('contributor_count')
        ]).select_from(
            select_capacity
        ).group_by(
            select_capacity.c.id,
            select_capacity.c.measurement_date
        ).order_by(
            select_capacity.c.id,
            select_capacity.c.measurement_date.desc()
        ).alias()
        return select([
            capacity_metrics.c.id,
            func.json_agg(
                func.json_build_object(
                    'measurement_date', capacity_metrics.c.measurement_date,
                    'measurement_window', measurement_window,
                    'total_commit_days', capacity_metrics.c.total_commit_days,
                    'avg_commit_days', capacity_metrics.c.avg_commit_days,
                    'min_commit_days', capacity_metrics.c.min_commit_days,
                    'max_commit_days', capacity_metrics.c.max_commit_days,
                    'contributor_count', capacity_metrics.c.contributor_count
                )
            ).label('capacity_trends')
        ]).select_from(
            capacity_metrics,
        ).group_by(
            capacity_metrics.c.id
        )

    @classmethod
    def get_contributor_level_capacity_trends(cls, measurement_window, project_nodes, timeline_dates):

        # Note: we are doing a very different query strategy here compared to
        # other trending interfaces. Rather than joining against projects_timeline_dates,
        # derived from joining project_nodes to timeline_dates, we are doing a cross join of the projects table
        # with timelinedates and using the commit date filter to select commits, and *then* limiting by the
        # id's in the project_nodes. We are doing this because
        # the regular approach was leading to query plans in prod where it was always doing a table scan on
        # the commits table. For some reason, the  query planner was ignoring the indexes on commit_date and repository
        # id and selecting the commits in scope using a full table scan of commmits, which is obviously slow.
        # the current strategy is about 500x faster as result. Dont have a clear explanation as to why
        # the other plan was so bad, but going with this approach here since it is functionally equivalent, even if a
        # bit less obvious in a logical sense.

        select_capacity = select([
            projects.c.id,
            timeline_dates.c.measurement_date,
            commits.c.author_contributor_key.label('contributor_key'),
            commits.c.author_contributor_name.label('contributor_name'),
            commit_day(commits).label('commit_day')
        ]).select_from(
            projects.join(
                timeline_dates, true()
            ).join(
                projects_repositories, projects_repositories.c.project_id == projects.c.id
            ).join(
                work_items_sources, work_items_sources.c.project_id == projects.c.id
            ).join(
                work_items, work_items.c.work_items_source_id == work_items_sources.c.id
            ).join(
                work_items_commits, work_items_commits.c.work_item_id == work_items.c.id
            ).join(
                commits, and_(
                    work_items_commits.c.commit_id == commits.c.id,
                    projects_repositories.c.repository_id == commits.c.repository_id
                )
            ).join(
                contributor_aliases, commits.c.author_contributor_alias_id == contributor_aliases.c.id
            )
        ).where(
            and_(
                date_column_is_in_measurement_window(
                    commits.c.commit_date,
                    measurement_date=timeline_dates.c.measurement_date,
                    measurement_window=measurement_window
                ),
                contributor_aliases.c.robot == False,
                projects_repositories.c.excluded == False,
                projects.c.id.in_(select([project_nodes.c.id]))
            )
        ).alias()

        capacity_metrics = select([
            select_capacity.c.id,
            select_capacity.c.measurement_date,
            select_capacity.c.contributor_key,
            select_capacity.c.contributor_name,
            func.count(select_capacity.c.commit_day.distinct()).label('total_commit_days'),
        ]).select_from(
            select_capacity
        ).group_by(
            select_capacity.c.id,
            select_capacity.c.measurement_date,
            select_capacity.c.contributor_key,
            select_capacity.c.contributor_name,
        ).order_by(
            select_capacity.c.id,
            select_capacity.c.measurement_date.desc()
        ).alias()
        return select([
            capacity_metrics.c.id,
            func.json_agg(
                func.json_build_object(
                    'measurement_date', capacity_metrics.c.measurement_date,
                    'measurement_window', measurement_window,
                    'contributor_key', capacity_metrics.c.contributor_key,
                    'contributor_name', capacity_metrics.c.contributor_name,
                    'total_commit_days', capacity_metrics.c.total_commit_days,
                )
            ).label('contributor_detail')
        ]).select_from(
            capacity_metrics,
        ).group_by(
            capacity_metrics.c.id
        )

    @classmethod
    def interface_selector(cls, project_nodes, **kwargs):
        capacity_trends_args = kwargs.get('capacity_trends_args')

        measurement_window = capacity_trends_args.measurement_window
        if measurement_window is None:
            raise ProcessingException(
                "'measurement_window' must be specified when calculating CapacityTrends"
            )

        # Get the a list of dates for trending using the trends_args for control
        timeline_dates = get_timeline_dates_for_trending(
            capacity_trends_args,
            arg_name='capacity_trends',
            interface_name='CapacityTrends'
        )

        if capacity_trends_args.show_contributor_detail:
            capacity_trends = cls.get_aggregate_capacity_trends(measurement_window, project_nodes,
                                                                timeline_dates).alias()
            contributor_detail = cls.get_contributor_level_capacity_trends(measurement_window,
                                                                           project_nodes, timeline_dates).alias()

            return select([
                project_nodes.c.id,
                capacity_trends.c.capacity_trends,
                contributor_detail.c.contributor_detail,
            ]).select_from(
                project_nodes.outerjoin(
                    capacity_trends, capacity_trends.c.id == project_nodes.c.id
                ).outerjoin(
                    contributor_detail, contributor_detail.c.id == project_nodes.c.id
                )
            )
        else:
            return cls.get_aggregate_capacity_trends(measurement_window, project_nodes, timeline_dates)


class ProjectPipelinePullRequestMetrics(InterfaceResolver):
    interface = PipelinePullRequestMetrics

    @classmethod
    def interface_selector(cls, project_nodes, **kwargs):
        pull_request_metrics_args = kwargs.get('pipeline_pull_request_metrics_args')

        age_target_percentile = pull_request_metrics_args.pull_request_age_target_percentile

        measurement_date = datetime.utcnow()

        pull_request_attributes = select([
            project_nodes.c.id,
            pull_requests.c.id.label('pull_request_id'),
            pull_requests.c.state.label('state'),
            (func.extract('epoch', measurement_date - pull_requests.c.created_at) / (1.0 * 3600 * 24)).label('age'),
        ]).select_from(
            project_nodes.join(
                projects_repositories, project_nodes.c.id == projects_repositories.c.project_id
            ).join(
                pull_requests, pull_requests.c.repository_id == projects_repositories.c.repository_id
            )
        ).where(
            and_(
                pull_requests.c.state == 'open',
                projects_repositories.c.excluded == False
            )
        ).alias('pull_request_attributes')

        pull_request_metrics = select([
            project_nodes.c.id.label('project_id'),
            func.avg(pull_request_attributes.c.age).label('avg_age'),
            func.min(pull_request_attributes.c.age).label('min_age'),
            func.max(pull_request_attributes.c.age).label('max_age'),
            func.percentile_disc(age_target_percentile).within_group(pull_request_attributes.c.age).label(
                'percentile_age'),
            func.count(pull_request_attributes.c.pull_request_id).filter(
                or_(
                    pull_request_attributes.c.state == 'closed',
                    pull_request_attributes.c.state == 'merged'
                )
            ).label('total_closed'),
            func.count(pull_request_attributes.c.pull_request_id).filter(
                pull_request_attributes.c.state == 'open'
            ).label('total_open')
        ]).select_from(
            project_nodes.outerjoin(
                pull_request_attributes, project_nodes.c.id == pull_request_attributes.c.id
            )
        ).group_by(
            project_nodes.c.id
        ).alias('pull_request_metrics')

        return select([
            pull_request_metrics.c.project_id.label('id'),
            func.json_agg(
                func.json_build_object(
                    'measurement_date', cast(measurement_date, Date),
                    'total_open', pull_request_metrics.c.total_open,
                    'total_closed', pull_request_metrics.c.total_closed,
                    'avg_age', pull_request_metrics.c.avg_age,
                    'min_age', pull_request_metrics.c.min_age,
                    'max_age', pull_request_metrics.c.max_age,
                    'percentile_age', pull_request_metrics.c.percentile_age
                )
            ).label('pipeline_pull_request_metrics')
        ]).select_from(
            pull_request_metrics
        ).group_by(
            pull_request_metrics.c.project_id
        )


class ProjectPullRequestMetricsTrends(InterfaceResolver):
    interface = PullRequestMetricsTrends

    @staticmethod
    def pull_request_attributes_all(measurement_window, project_timeline_dates, pull_request_attribute_cols):
        pull_request_attributes = select(pull_request_attribute_cols).select_from(
            project_timeline_dates.outerjoin(
                projects_repositories, project_timeline_dates.c.id == projects_repositories.c.project_id
            ).join(
                repositories, repositories.c.id == projects_repositories.c.repository_id
            ).join(
                pull_requests, pull_requests.c.repository_id == repositories.c.id
            )
        ).where(
            and_(
                pull_requests.c.end_date != None,
                projects_repositories.c.excluded == False,
                date_column_is_in_measurement_window(
                    pull_requests.c.end_date,
                    measurement_date=project_timeline_dates.c.measurement_date,
                    measurement_window=measurement_window
                )
            )
        ).group_by(
            project_timeline_dates.c.id,
            project_timeline_dates.c.measurement_date,
            pull_requests.c.id
        ).alias('pull_request_attributes')
        return pull_request_attributes

    @staticmethod
    def pull_request_attributes_specs(measurement_window, project_timeline_dates, pull_request_attribute_cols):
        pull_request_attributes = select(pull_request_attribute_cols).distinct().select_from(
            project_timeline_dates.join(
                work_items_sources, work_items_sources.c.project_id == project_timeline_dates.c.id
            ).join(
                work_items, work_items.c.work_items_source_id == work_items_sources.c.id
            ).join(
                work_items_pull_requests, work_items_pull_requests.c.work_item_id == work_items.c.id
            ).join(
                pull_requests, work_items_pull_requests.c.pull_request_id == pull_requests.c.id
            ).join(
                repositories, pull_requests.c.repository_id == repositories.c.id
            ).join(
                projects_repositories,
                and_(
                    repositories.c.id == projects_repositories.c.repository_id,
                    project_timeline_dates.c.id == projects_repositories.c.project_id
                )
            )
        ).where(
            and_(
                pull_requests.c.end_date != None,
                projects_repositories.c.excluded == False,
                date_column_is_in_measurement_window(
                    pull_requests.c.end_date,
                    measurement_date=project_timeline_dates.c.measurement_date,
                    measurement_window=measurement_window
                )
            )
        ).group_by(
            project_timeline_dates.c.id,
            project_timeline_dates.c.measurement_date,
            pull_requests.c.id
        ).alias('pull_request_attributes')
        return pull_request_attributes

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        pull_request_metrics_trends_args = kwargs.get('pull_request_metrics_trends_args')
        age_target_percentile = pull_request_metrics_trends_args.pull_request_age_target_percentile
        # Get the list of dates for trending using the trends_args for control
        timeline_dates = get_timeline_dates_for_trending(
            pull_request_metrics_trends_args,
            arg_name='pull_request_metrics_trends',
            interface_name='PullRequestMetricsTrends'
        )
        project_timeline_dates = select([project_nodes.c.id, timeline_dates]).cte()

        measurement_window = pull_request_metrics_trends_args.measurement_window
        if measurement_window is None:
            raise ProcessingException(
                "'measurement_window' must be specified when calculating ProjectPullRequestMetricsTrends"
            )

        pull_request_attribute_cols = [
            project_timeline_dates.c.id,
            project_timeline_dates.c.measurement_date,
            pull_requests.c.id.label('pull_request_id'),
            pull_requests.c.state.label('state'),
            pull_requests.c.end_date,
            (func.extract('epoch', pull_requests.c.end_date - pull_requests.c.created_at) / (1.0 * 3600 * 24)).label(
                'age'),
        ]

        if pull_request_metrics_trends_args.get('specs_only'):
            pull_request_attributes = ProjectPullRequestMetricsTrends.pull_request_attributes_specs(
                measurement_window, project_timeline_dates, pull_request_attribute_cols)
        else:
            pull_request_attributes = ProjectPullRequestMetricsTrends.pull_request_attributes_all(measurement_window,
                                                                                                  project_timeline_dates,
                                                                                                  pull_request_attribute_cols)
        pull_request_metrics = select([
            project_timeline_dates.c.id,
            project_timeline_dates.c.measurement_date,
            func.avg(pull_request_attributes.c.age).label('avg_age'),
            func.min(pull_request_attributes.c.age).label('min_age'),
            func.max(pull_request_attributes.c.age).label('max_age'),
            func.percentile_disc(age_target_percentile).within_group(pull_request_attributes.c.age).label(
                'percentile_age'),
            func.count(pull_request_attributes.c.pull_request_id).label('total_closed'),
            literal(0).label('total_open')
        ]).select_from(
            project_timeline_dates.outerjoin(
                pull_request_attributes, and_(
                    project_timeline_dates.c.id == pull_request_attributes.c.id,
                    project_timeline_dates.c.measurement_date == pull_request_attributes.c.measurement_date
                )
            )).group_by(
            project_timeline_dates.c.measurement_date,
            project_timeline_dates.c.id
        ).order_by(
            project_timeline_dates.c.id,
            project_timeline_dates.c.measurement_date.desc()
        ).alias('pull_request_metrics')

        return select([
            pull_request_metrics.c.id,
            func.json_agg(
                func.json_build_object(
                    'measurement_date', cast(pull_request_metrics.c.measurement_date, Date),
                    'measurement_window', measurement_window,
                    'total_open', func.coalesce(pull_request_metrics.c.total_open, 0),
                    'total_closed', func.coalesce(pull_request_metrics.c.total_closed, 0),
                    'avg_age', func.coalesce(pull_request_metrics.c.avg_age, 0),
                    'min_age', func.coalesce(pull_request_metrics.c.min_age, 0),
                    'max_age', func.coalesce(pull_request_metrics.c.max_age, 0),
                    'percentile_age', func.coalesce(pull_request_metrics.c.percentile_age, 0)
                )
            ).label('pull_request_metrics_trends')
        ]).select_from(
            pull_request_metrics
        ).group_by(
            pull_request_metrics.c.id
        )


class ProjectPullRequestNodes(ConnectionResolver):
    interfaces = (NamedNode, PullRequestInfo)

    @staticmethod
    def pull_requests_traceable_to_project():
        return projects.join(
            work_items_sources, work_items_sources.c.project_id == projects.c.id
        ).join(
            work_items, work_items.c.work_items_source_id == work_items_sources.c.id
        ).join(
            work_items_pull_requests, work_items_pull_requests.c.work_item_id == work_items.c.id
        ).join(
            pull_requests, work_items_pull_requests.c.pull_request_id == pull_requests.c.id
        ).join(
            repositories, pull_requests.c.repository_id == repositories.c.id
        ).join(
            projects_repositories,
            and_(
                repositories.c.id == projects_repositories.c.repository_id,
                projects.c.id == projects_repositories.c.project_id
            )
        )

    @staticmethod
    def all_pull_requests_for_project_repos():
        return projects.join(
            projects_repositories, projects_repositories.c.project_id == projects.c.id
        ).join(
            repositories, projects_repositories.c.repository_id == repositories.c.id
        ).join(
            pull_requests, pull_requests.c.repository_id == repositories.c.id
        )

    @staticmethod
    def connection_nodes_selector(**kwargs):
        if kwargs.get('specs_only'):
            pull_requests_join_clause = ProjectPullRequestNodes.pull_requests_traceable_to_project()
        else:
            pull_requests_join_clause = ProjectPullRequestNodes.all_pull_requests_for_project_repos()

        select_pull_requests = select([
            *pull_request_info_columns(pull_requests)
        ]).distinct().select_from(
            pull_requests_join_clause
        ).where(
            and_(
                projects.c.key == bindparam('key'),
                projects_repositories.c.excluded == False
            )
        )

        return pull_requests_connection_apply_filters(select_pull_requests, **kwargs)



    @staticmethod
    def sort_order(pull_request_nodes, **kwargs):
        return [pull_request_nodes.c.created_at.desc().nullsfirst()]


class ProjectFlowRateTrends(InterfaceResolver):
    interface = FlowRateTrends

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        flow_rate_trends_args = kwargs.get('flow_rate_trends_args')

        # Get the a list of dates for trending using the trends_args for control
        timeline_dates = get_timeline_dates_for_trending(
            flow_rate_trends_args,
            arg_name='flow_rate_trends',
            interface_name='FlowRateTrends'
        )

        project_timeline_dates = select([project_nodes.c.id, timeline_dates]).cte()

        measurement_window = flow_rate_trends_args.measurement_window
        if measurement_window is None:
            raise ProcessingException(
                "'measurement_window' must be specified when calculating ProjectCycleMetricsTrends"
            )

        flow_rate_trends = select([
            project_timeline_dates.c.id,
            project_timeline_dates.c.measurement_date,
            func.count(work_item_delivery_cycles.c.delivery_cycle_id).filter(
                date_column_is_in_measurement_window(
                    work_item_delivery_cycles.c.start_date,
                    measurement_date=project_timeline_dates.c.measurement_date,
                    measurement_window=measurement_window
                )
            ).label('arrival_rate'),
            func.count(work_item_delivery_cycles.c.delivery_cycle_id).filter(
                date_column_is_in_measurement_window(
                    work_item_delivery_cycles.c.end_date,
                    measurement_date=project_timeline_dates.c.measurement_date,
                    measurement_window=measurement_window
                )
            ).label('close_rate')
        ]).select_from(
            project_timeline_dates.join(
                work_items_sources, work_items_sources.c.project_id == project_timeline_dates.c.id
            ).join(
                work_items, work_items.c.work_items_source_id == work_items_sources.c.id
            ).join(
                work_item_delivery_cycles, work_item_delivery_cycles.c.work_item_id == work_items.c.id
            )
        )

        flow_rate_trends = apply_specs_only_filter(flow_rate_trends, work_items, work_item_delivery_cycles,
                                                   **flow_rate_trends_args)
        flow_rate_trends = apply_defects_only_filter(flow_rate_trends, work_items, **flow_rate_trends_args)

        flow_rate_trends = apply_releases_filter(flow_rate_trends, work_items, **flow_rate_trends_args)

        flow_rate_trends = apply_tags_filter(flow_rate_trends, work_items, **flow_rate_trends_args)

        flow_rate_trends = flow_rate_trends.group_by(
            project_timeline_dates.c.id,
            project_timeline_dates.c.measurement_date
        ).distinct().alias('flow_rate_trends')

        return select([
            project_timeline_dates.c.id,
            func.json_agg(
                func.json_build_object(
                    'measurement_date', cast(project_timeline_dates.c.measurement_date, Date),
                    'measurement_window', measurement_window,
                    'arrival_rate', flow_rate_trends.c.arrival_rate,
                    'close_rate', flow_rate_trends.c.close_rate
                )
            ).label('flow_rate_trends')
        ]).select_from(
            project_timeline_dates.outerjoin(
                flow_rate_trends,
                and_(
                    flow_rate_trends.c.id == project_timeline_dates.c.id,
                    flow_rate_trends.c.measurement_date == project_timeline_dates.c.measurement_date
                )
            )
        ).group_by(
            project_timeline_dates.c.id
        )

class ProjectArrivalDepartureTrends(InterfaceResolver):
    interface = ArrivalDepartureTrends

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        arrival_departure_trends_args = kwargs.get('arrival_departure_trends_args')
        if arrival_departure_trends_args is None:
            raise ProcessingException('arrivalDepartureTrendsArgs parameter must be specified to resolve ArrivalDepartureTrendsInterface')

        timeline_dates = get_timeline_dates_for_trending(
            arrival_departure_trends_args,
            arg_name='arrival_departure_trends',
            interface_name='ArrivalDepartureTrends'
        )
        project_nodes_dates = select([project_nodes, timeline_dates]).cte('project_nodes_dates')

        measurement_window = arrival_departure_trends_args.measurement_window
        if measurement_window is None:
            raise ProcessingException(
                "'measurement_window' must be specified when calculating ProjectWipArrivalRateTrends"
            )

        current_state_map = work_items_source_state_map.alias('current_state_map')
        previous_state_map = work_items_source_state_map.alias('previous_state_map')

        select_arrivals = select([
            project_nodes_dates.c.id.label('project_id'),
            project_nodes_dates.c.measurement_date,
            work_item_delivery_cycles.c.delivery_cycle_id,
            work_items.c.display_id,
            work_item_state_transitions.c.created_at,
            work_item_state_transitions.c.previous_state,
            previous_state_map.c.state_type.label('previous_state_type'),
            work_item_state_transitions.c.state.label('current_state'),
            current_state_map.c.state_type.label('current_state_type')
        ]).select_from(
            project_nodes_dates.join(
                work_items_sources, work_items_sources.c.project_id == project_nodes_dates.c.id
            ).join(
                work_items, work_items.c.work_items_source_id == work_items_sources.c.id
            ).join(
                work_item_delivery_cycles, work_item_delivery_cycles.c.work_item_id == work_items.c.id,
            ).join(
                current_state_map, current_state_map.c.work_items_source_id == work_items_sources.c.id,
            ).join(
                previous_state_map, previous_state_map.c.work_items_source_id == work_items_sources.c.id,
            ).join(
                work_item_state_transitions,
                and_(
                    work_item_state_transitions.c.work_item_id == work_items.c.id,
                    # Map state type for current and previous states
                    current_state_map.c.state == work_item_state_transitions.c.state,
                    previous_state_map, previous_state_map.c.state == work_item_state_transitions.c.previous_state,
                    # match up transitions to the delivery cycles in which they belong
                    work_item_delivery_cycles.c.start_seq_no <= work_item_state_transitions.c.seq_no,
                    or_(
                        work_item_delivery_cycles.c.end_seq_no == None,
                        work_item_state_transitions.c.seq_no <= work_item_delivery_cycles.c.end_seq_no
                    )
                )
            )
        ).where(
            and_(
                date_column_is_in_measurement_window(
                    work_item_state_transitions.c.created_at,
                    measurement_date=project_nodes_dates.c.measurement_date,
                    measurement_window=measurement_window
                )
            )
        )
        # apply all the regular filters for work items and delivery cycles.
        select_arrivals = work_item_delivery_cycles_connection_apply_filters(select_arrivals, work_items, work_item_delivery_cycles, **arrival_departure_trends_args).alias('select_arrivals')

        # add the where clauses to filter the transitions we are interested in
        metric = arrival_departure_trends_args.get('metric')
        if metric is None:
            raise ProcessingException(
                "Required parameter 'metric' was not provided for resolving interface ArrivalDepartureTrends")
        arrivals_filter = None
        departures_filter = None
        flowbacks_filter = None

        if metric == ArrivalDepartureMetricsEnum.wip_arrivals_departures.value:
            arrivals_filter = and_(
                # we consider transitions from non-wip phases to wip phases to be arrivals.
                select_arrivals.c.previous_state_type.in_(['backlog', 'closed']),
                select_arrivals.c.current_state_type.in_(['open', 'wip', 'complete'])
            )
            departures_filter = and_(
                # we consider transitions from wip phases  to closed phases to be arrivals.
                select_arrivals.c.previous_state_type.in_(['open', 'wip', 'complete']),
                select_arrivals.c.current_state_type.in_(['closed'])
            )
            flowbacks_filter = and_(
                # we consider transitions from wip phases to define phases to be arrivals.
                select_arrivals.c.previous_state_type.in_(['open', 'wip', 'complete']),
                select_arrivals.c.current_state_type.in_(['backlog'])
            )
            passthroughs_filter = and_(
                # we consider transitions from wip phases to define phases to be arrivals.
                select_arrivals.c.previous_state_type.in_(['backlog']),
                select_arrivals.c.current_state_type.in_(['closed'])
            )

        select_arrival_rate_trends = select([
            project_nodes_dates.c.id,
            project_nodes_dates.c.measurement_date,
            func.count(select_arrivals.c.delivery_cycle_id.distinct()).filter(
                arrivals_filter
            ).label('arrivals'),
            func.count(select_arrivals.c.delivery_cycle_id.distinct()).filter(
                departures_filter
            ).label('departures'),
            func.count(select_arrivals.c.delivery_cycle_id.distinct()).filter(
                flowbacks_filter
            ).label('flowbacks'),
            func.count(select_arrivals.c.delivery_cycle_id.distinct()).filter(
                passthroughs_filter
            ).label('passthroughs')
        ]).select_from(
            project_nodes_dates.join(
                select_arrivals,
                and_(
                    select_arrivals.c.project_id == project_nodes_dates.c.id,
                    select_arrivals.c.measurement_date == project_nodes_dates.c.measurement_date
                )
            )
        ).group_by(
            project_nodes_dates.c.id,
            project_nodes_dates.c.measurement_date
        ).order_by(
            project_nodes_dates.c.id,
            project_nodes_dates.c.measurement_date.desc()
        ).alias('select_arrival_rate_trends')

        result = select([
            project_nodes_dates.c.id,
            func.json_agg(
                func.json_build_object(
                    'measurement_date', project_nodes_dates.c.measurement_date,
                    'measurement_window', measurement_window,
                    'arrivals', func.coalesce(select_arrival_rate_trends.c.arrivals,0),
                    'departures', func.coalesce(select_arrival_rate_trends.c.departures,0),
                    'flowbacks', func.coalesce(select_arrival_rate_trends.c.flowbacks, 0),
                    'passthroughs', func.coalesce(select_arrival_rate_trends.c.passthroughs,0)
                )
            ).label('arrival_departure_trends')

        ]).select_from(
            project_nodes_dates.outerjoin(
                select_arrival_rate_trends,
                and_(
                    project_nodes_dates.c.id == select_arrival_rate_trends.c.id,
                    project_nodes_dates.c.measurement_date == select_arrival_rate_trends.c.measurement_date
                )
            )
        ).group_by(
            project_nodes_dates.c.id
        )

        return result




class ProjectBacklogTrends(InterfaceResolver):
    interface = BacklogTrends

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        backlog_trends_args = kwargs.get('backlog_trends_args')

        # Get the list of dates for trending using the trends_args for control
        timeline_dates = get_timeline_dates_for_trending(
            backlog_trends_args,
            arg_name='backlog_trends',
            interface_name='BacklogTrends'
        )

        project_timeline_dates = select([project_nodes.c.id, timeline_dates]).cte()

        measurement_window = backlog_trends_args.measurement_window
        if measurement_window is None:
            raise ProcessingException(
                "'measurement_window' must be specified when calculating ProjectCycleMetricsTrends"
            )
        measurement_period_start_date, measurement_period_end_date = get_measurement_period(
            backlog_trends_args,
            arg_name='backlog_trends',
            interface_name='BacklogTrends'
        )

        all_dates_in_period = select([
            cast(
                func.generate_series(
                    measurement_period_end_date,
                    measurement_period_start_date - timedelta(days=measurement_window),
                    timedelta(days=-1)
                ),
                Date
            ).label('date_of_window')
        ]).alias()

        daily_backlog_counts = select([
            project_nodes.c.id.label('project_id'),
            all_dates_in_period.c.date_of_window,
            func.count(work_items.c.id).label('backlog_size')
        ]).select_from(
            work_items.join(
                work_item_delivery_cycles, work_items.c.id == work_item_delivery_cycles.c.work_item_id
            ).join(
                work_items_sources, work_items_sources.c.id == work_items.c.work_items_source_id
            ).join(
                project_nodes, project_nodes.c.id == work_items_sources.c.project_id
            ).join(
                all_dates_in_period, literal(True)
            )
        ).where(
            or_(
                work_item_delivery_cycles.c.end_date == None,
                work_item_delivery_cycles.c.end_date > all_dates_in_period.c.date_of_window
            )
        )

        daily_backlog_counts = apply_defects_only_filter(daily_backlog_counts, work_items, **backlog_trends_args)
        daily_backlog_counts = apply_specs_only_filter(daily_backlog_counts, work_items, work_item_delivery_cycles,
                                                       **backlog_trends_args)

        daily_backlog_counts = daily_backlog_counts.group_by(
            project_nodes.c.id,
            all_dates_in_period.c.date_of_window
        ).cte()

        daily_backlog_counts_timeline_dates = select([
            daily_backlog_counts.c.project_id,
            daily_backlog_counts.c.date_of_window,
            daily_backlog_counts.c.backlog_size,
            timeline_dates.c.measurement_date
        ]).select_from(
            daily_backlog_counts.join(
                timeline_dates, literal(True)
            )
        ).where(
            and_(
                daily_backlog_counts.c.date_of_window <= timeline_dates.c.measurement_date,
                daily_backlog_counts.c.date_of_window > timeline_dates.c.measurement_date - timedelta(
                    days=measurement_window)
            )
        ).group_by(
            timeline_dates.c.measurement_date,
            daily_backlog_counts.c.date_of_window,
            daily_backlog_counts.c.project_id,
            daily_backlog_counts.c.backlog_size
        ).alias()

        window_backlog_counts = daily_backlog_counts.alias()

        backlog_trends = select([
            daily_backlog_counts_timeline_dates.c.project_id.label('id'),
            daily_backlog_counts_timeline_dates.c.measurement_date,
            window_backlog_counts.c.backlog_size,
            func.min(daily_backlog_counts_timeline_dates.c.backlog_size).label('min_backlog_size'),
            func.max(daily_backlog_counts_timeline_dates.c.backlog_size).label('max_backlog_size'),
            func.avg(daily_backlog_counts_timeline_dates.c.backlog_size).label('avg_backlog_size'),
            func.percentile_disc(0.25).within_group(daily_backlog_counts_timeline_dates.c.backlog_size).label(
                'q1_backlog_size'),
            func.percentile_disc(0.5).within_group(daily_backlog_counts_timeline_dates.c.backlog_size).label(
                'median_backlog_size'),
            func.percentile_disc(0.75).within_group(daily_backlog_counts_timeline_dates.c.backlog_size).label(
                'q3_backlog_size')
        ]).select_from(
            daily_backlog_counts_timeline_dates.join(
                window_backlog_counts,
                window_backlog_counts.c.date_of_window == daily_backlog_counts_timeline_dates.c.measurement_date
            )
        ).group_by(
            daily_backlog_counts_timeline_dates.c.project_id,
            daily_backlog_counts_timeline_dates.c.measurement_date,
            window_backlog_counts.c.date_of_window,
            window_backlog_counts.c.backlog_size
        ).order_by(
            desc(daily_backlog_counts_timeline_dates.c.measurement_date)
        ).distinct().alias('backlog_trends')

        return select([
            project_timeline_dates.c.id,
            func.json_agg(
                func.json_build_object(
                    'measurement_date', cast(project_timeline_dates.c.measurement_date, Date),
                    'measurement_window', measurement_window,
                    'backlog_size', backlog_trends.c.backlog_size,
                    'min_backlog_size', backlog_trends.c.min_backlog_size,
                    'max_backlog_size', backlog_trends.c.max_backlog_size,
                    'q1_backlog_size', backlog_trends.c.q1_backlog_size,
                    'q3_backlog_size', backlog_trends.c.q3_backlog_size,
                    'median_backlog_size', backlog_trends.c.median_backlog_size,
                    'avg_backlog_size', backlog_trends.c.avg_backlog_size
                )
            ).label('backlog_trends')
        ]).select_from(
            project_timeline_dates.outerjoin(
                backlog_trends,
                and_(
                    backlog_trends.c.id == project_timeline_dates.c.id,
                    backlog_trends.c.measurement_date == project_timeline_dates.c.measurement_date
                )
            )
        ).group_by(
            project_timeline_dates.c.id
        )

class ProjectTags(InterfaceResolver):
    interface = Tags

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        project_tags = select([
            project_nodes.c.id,
            func.unnest(work_items.c.tags).label('tag')
        ]).distinct().select_from(
            project_nodes.join(
                work_items_sources, work_items_sources.c.project_id == project_nodes.c.id
            ).join(
                work_items, work_items.c.work_items_source_id == work_items_sources.c.id
            )
        ).cte()
        return select([
            project_tags.c.id,
            func.array_agg(project_tags.c.tag).label('tags')
        ]).select_from(
            project_tags
        ).group_by(
            project_tags.c.id
        )

class ProjectReleases(InterfaceResolver):
    interface = Releases

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        releases_active_within_days = kwargs.get('releases_active_within_days')
        if releases_active_within_days is None:
            project_releases = select([
                project_nodes.c.id,
                func.unnest(work_items.c.releases).label('release')
            ]).distinct().select_from(
                project_nodes.join(
                    work_items_sources, work_items_sources.c.project_id == project_nodes.c.id
                ).join(
                    work_items, work_items.c.work_items_source_id == work_items_sources.c.id
                )
            ).cte()
        else:
            project_releases = select([
                project_nodes.c.id,
                func.unnest(work_items.c.releases).label('release')
            ]).distinct().select_from(
                project_nodes.join(
                    work_items_sources, work_items_sources.c.project_id == project_nodes.c.id
                ).join(
                    work_items, work_items.c.work_items_source_id == work_items_sources.c.id
                ).join(
                    work_item_state_transitions, work_item_state_transitions.c.work_item_id == work_items.c.id
                )
            ).where(
                work_item_state_transitions.c.created_at >= datetime.utcnow() - timedelta(days=releases_active_within_days)
            ).cte()



        return select([
            project_releases.c.id,
            func.array_agg(project_releases.c.release).label('releases')
        ]).select_from(
            project_releases
        ).group_by(
            project_releases.c.id
        )