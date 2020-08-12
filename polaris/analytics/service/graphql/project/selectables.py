# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from datetime import datetime, timedelta
import abc

from polaris.common import db

from sqlalchemy import select, func, bindparam, distinct, and_, cast, Text, between, extract, case, literal_column, \
    union_all, literal, Date, true

from polaris.analytics.db.enums import JiraWorkItemType, WorkItemTypesToIncludeInCycleMetrics
from polaris.analytics.db.model import projects, projects_repositories, organizations, \
    repositories, contributors, \
    contributor_aliases, repositories_contributor_aliases, commits, work_items_sources, \
    work_items, work_item_state_transitions, work_items_commits, work_item_delivery_cycles, work_items_source_state_map, \
    work_item_delivery_cycle_durations

from polaris.graphql.base_classes import NamedNodeResolver, InterfaceResolver, ConnectionResolver, \
    SelectableFieldResolver
from polaris.graphql.interfaces import NamedNode
from polaris.graphql.utils import nulls_to_zero
from polaris.utils.collections import dict_merge
from polaris.utils.datetime_utils import time_window
from polaris.utils.exceptions import ProcessingException
from polaris.analytics.db.enums import WorkItemsStateType
from .sql_expressions import get_timeline_dates_for_trending
from ..commit.sql_expressions import commit_info_columns, commits_connection_apply_time_window_filters
from ..contributor.sql_expressions import contributor_count_apply_contributor_days_filter
from ..interfaces import \
    CommitSummary, ContributorCount, RepositoryCount, OrganizationRef, CommitCount, \
    CumulativeCommitCount, CommitInfo, WeeklyContributorCount, ArchivedStatus, \
    WorkItemEventSpan, WorkItemsSourceRef, WorkItemInfo, WorkItemStateTransition, WorkItemCommitInfo, \
    WorkItemStateTypeCounts, AggregateCycleMetrics, DeliveryCycleInfo, CycleMetricsTrends, PipelineCycleMetricsTrends, \
    TraceabilityTrends

from ..work_item import sql_expressions
from ..work_item.sql_expressions import work_item_events_connection_apply_time_window_filters, work_item_event_columns, \
    work_item_info_columns, work_item_commit_info_columns, work_items_connection_apply_filters, \
    work_item_delivery_cycle_info_columns, work_item_delivery_cycles_connection_apply_filters, \
    work_item_info_group_expr_columns


class ProjectNode(NamedNodeResolver):
    interfaces = (NamedNode, ArchivedStatus)

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            projects.c.id,
            projects.c.key.label('key'),
            projects.c.name,
            projects.c.archived
        ]).select_from(
            projects
        ).where(projects.c.key == bindparam('key'))


class ProjectRepositoriesNodes(ConnectionResolver):
    interface = NamedNode

    @staticmethod
    def connection_nodes_selector(**kwargs):
        return select([
            repositories.c.id,
            repositories.c.key,
            repositories.c.name
        ]).select_from(
            projects.join(
                projects_repositories
            ).join(
                repositories
            )
        ).where(projects.c.key == bindparam('key'))


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
            *commit_info_columns(repositories, commits)
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
            )
        ).where(
            projects.c.key == bindparam('key')
        )
        project_commits = commits_connection_apply_time_window_filters(select_project_commits, commits, **kwargs)

        select_untracked_commits = select([
            *commit_info_columns(repositories, commits)
        ]).select_from(
            projects.join(
                projects_repositories, projects_repositories.c.project_id == projects.c.id,
            ).join(
                repositories, projects_repositories.c.repository_id == repositories.c.id,
            ).join(
                commits, commits.c.repository_id == repositories.c.id
            )
        ).where(
            and_(
                projects.c.key == bindparam('key'),
                commits.c.work_items_summaries == None
            )
        )

        untracked_commits = commits_connection_apply_time_window_filters(select_untracked_commits, commits, **kwargs)
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
        return select([
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
                repositories_contributor_aliases.c.robot == False
            )
        ).distinct()


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
                contributor_aliases.c.robot == False
            )
        ).group_by(
            commits.c.author_contributor_key
        )

    @staticmethod
    def sort_order(recently_active_contributors, **kwargs):
        return [recently_active_contributors.c.commit_count.desc()]


class ProjectWorkItemNodes(ConnectionResolver):
    interfaces = (NamedNode, WorkItemInfo, WorkItemsSourceRef)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        select_stmt = select([
            work_items_sources.c.key.label('work_items_source_key'),
            work_items_sources.c.name.label('work_items_source_name'),
            work_items_sources.c.integration_type.label('work_tracking_integration_type'),
            work_items.c.name,
            work_items.c.key,
            *work_item_info_columns(work_items),
            work_items.c.id
        ]).select_from(
            projects.join(
                work_items_sources, work_items_sources.c.project_id == projects.c.id
            ).join(
                work_items
            )
        ).where(
            projects.c.key == bindparam('key')
        )
        return work_items_connection_apply_filters(select_stmt, work_items, **kwargs)

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
        return commits_connection_apply_time_window_filters(select_stmt, commits, **kwargs)

    @staticmethod
    def sort_order(project_work_item_commits_nodes, **kwargs):
        return [project_work_item_commits_nodes.c.commit_date.desc()]


class ProjectWorkItemDeliveryCycleNodes(ConnectionResolver):
    interfaces = (NamedNode, WorkItemInfo, DeliveryCycleInfo)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        select_stmt = select([
            work_item_delivery_cycles.c.delivery_cycle_id.label('id'),
            *work_item_delivery_cycle_info_columns(work_items, work_item_delivery_cycles),
            *work_item_info_columns(work_items),
        ]).select_from(
            projects.join(
                work_items_sources, work_items_sources.c.project_id == projects.c.id
            ).join(
                work_items, work_items.c.work_items_source_id == work_items_sources.c.id
            ).join(
                work_item_delivery_cycles, work_item_delivery_cycles.c.work_item_id == work_items.c.id
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
            projects.c.key == bindparam('key')
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
            projects.c.key == bindparam('key')
        ).group_by(
            extract('year', commits.c.commit_date),
            extract('week', commits.c.commit_date)
        )


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
        ).group_by(project_nodes.c.id)

    @staticmethod
    def sort_order(projects_commit_summary, **kwargs):
        return [nulls_to_zero(projects_commit_summary.c.commit_count).desc()]


class ProjectsContributorCount(InterfaceResolver):
    interface = ContributorCount

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        select_stmt = select([
            project_nodes.c.id,
            func.count(distinct(repositories_contributor_aliases.c.contributor_id)).label('contributor_count')
        ]).select_from(
            project_nodes.outerjoin(
                projects_repositories, projects_repositories.c.project_id == project_nodes.c.id
            ).outerjoin(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            ).outerjoin(
                repositories_contributor_aliases, repositories.c.id == repositories_contributor_aliases.c.repository_id
            )
        ).where(
            repositories_contributor_aliases.c.robot == False
        )
        select_stmt = contributor_count_apply_contributor_days_filter(select_stmt, **kwargs)

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


class ProjectWorkItemStateTypeCounts(InterfaceResolver):
    interface = WorkItemStateTypeCounts

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        select_work_items = select([
            project_nodes.c.id,
            case(
                [
                    (work_items.c.state_type == None, 'unmapped')
                ],
                else_=work_items.c.state_type
            ).label('state_type'),
            func.count(work_items.c.id).label('count')
        ]).select_from(
            project_nodes.join(
                work_items_sources, work_items_sources.c.project_id == project_nodes.c.id,
            ).join(
                work_items, work_items.c.work_items_source_id == work_items_sources.c.id
            )
        )
        if 'defects_only' in kwargs:
            select_work_items = select_work_items.where(work_items.c.is_bug == True)

        work_items_by_state_type = select_work_items.group_by(
            project_nodes.c.id,
            work_items.c.state_type
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
            ).label('work_item_state_type_counts')

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


class ProjectCycleMetricsTrendsBase(InterfaceResolver, abc.ABC):

    @classmethod
    def get_metrics_map(cls, cycle_metrics_trends_args, delivery_cycles_relation=work_item_delivery_cycles):
        return dict(
            # Cycle metrics
            cycle_metrics=dict(
                percentile_lead_time=func.percentile_disc(
                    cycle_metrics_trends_args.lead_time_target_percentile
                ).within_group(
                    delivery_cycles_relation.c.lead_time
                ).label(
                    'percentile_lead_time'
                ),
                percentile_cycle_time=func.percentile_disc(
                    cycle_metrics_trends_args.cycle_time_target_percentile
                ).within_group(
                    delivery_cycles_relation.c.cycle_time
                ).label(
                    'percentile_cycle_time'
                ),
                min_lead_time=func.min(delivery_cycles_relation.c.lead_time).label('min_lead_time'),
                avg_lead_time=func.avg(delivery_cycles_relation.c.lead_time).label('avg_lead_time'),
                max_lead_time=func.max(delivery_cycles_relation.c.lead_time).label('max_lead_time'),
                min_cycle_time=func.min(delivery_cycles_relation.c.cycle_time).label('min_cycle_time'),
                avg_cycle_time=func.avg(delivery_cycles_relation.c.cycle_time).label('avg_cycle_time'),
                q1_cycle_time=func.percentile_disc(
                    0.25
                ).within_group(
                    delivery_cycles_relation.c.cycle_time
                ).label(
                    'q1_cycle_time'
                ),
                median_cycle_time=func.percentile_disc(
                    0.50
                ).within_group(
                    delivery_cycles_relation.c.cycle_time
                ).label(
                    'median_cycle_time'
                ),
                q3_cycle_time=func.percentile_disc(
                    0.75
                ).within_group(
                    delivery_cycles_relation.c.cycle_time
                ).label(
                    'q3_cycle_time'
                ),
                max_cycle_time=func.max(delivery_cycles_relation.c.cycle_time).label('max_cycle_time'),
            ),
            # Work item counts
            work_item_counts=dict(
                work_items_in_scope=func.count(
                    delivery_cycles_relation.c.delivery_cycle_id
                ).label('work_items_in_scope'),

                work_items_with_null_cycle_time=func.sum(
                    case([
                        (
                            and_(
                                # we need this and clause here, because we are
                                # counting a value that is None, but since this
                                # is invoked typically in a outerjoin, we need to
                                # qualify it with some non-null value. This implementation
                                # work correctly only for the closed items case, but this is
                                # the only case where it makes sense to compute this metric anyway.
                                delivery_cycles_relation.c.end_date != None,
                                delivery_cycles_relation.c.cycle_time == None
                            ), 1
                        )
                    ], else_=0)
                ).label('work_items_with_null_cycle_time'),

                work_items_with_commits=func.sum(
                    case([
                        (
                            delivery_cycles_relation.c.commit_count > 0, 1
                        )
                    ], else_=0)
                ).label('work_items_with_commits')
            )
        )

    @classmethod
    def get_metrics_columns(cls, cycle_metrics_trends_args, metrics_map, metric_type):
        metric_type_map = metrics_map[metric_type]
        return [
            metric_type_map[metric]
            for metric in cycle_metrics_trends_args.metrics if metric in metric_type_map
        ]

    @classmethod
    def get_cycle_metrics_json_object_columns(cls, cycle_metrics_trends_args, metrics_map, cycle_metrics_query):
        columns = []
        for metric in cycle_metrics_trends_args.metrics:
            if metric in metrics_map['cycle_metrics']:
                columns.extend([metric, (cycle_metrics_query.c[metric] / (1.0 * 24 * 3600)).label(metric)])
        return columns


    @staticmethod
    def get_work_item_count_metrics_json_object_columns(cycle_metrics_trends_args, metrics_map,  cycle_metrics_query):
        columns = []
        for metric in cycle_metrics_trends_args.metrics:
            if metric in metrics_map['work_item_counts']:
                columns.extend([metric, cycle_metrics_query.c[metric]])
        return columns

    @staticmethod
    def get_work_item_filter_clauses(cycle_metrics_trends_args):

        columns = []
        if not cycle_metrics_trends_args.include_epics_and_subtasks:
            # the default value is false, so we filter out epics and subtasks unless it
            # it is explicitly requested.
            columns.append(
                work_items.c.work_item_type.notin_([
                    JiraWorkItemType.epic.value,
                    JiraWorkItemType.sub_task.value
                ]
                )
            )
        if cycle_metrics_trends_args.defects_only:
            columns.append(
                work_items.c.is_bug == True
            )
        return columns

    @staticmethod
    def get_work_item_delivery_cycle_filter_clauses(cycle_metrics_trends_args):

        columns = []
        if cycle_metrics_trends_args.specs_only:
            columns.append(work_item_delivery_cycles.c.commit_count > 0)

        return columns


class ProjectCycleMetricsTrends(ProjectCycleMetricsTrendsBase):
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
            project_nodes.c.id.label('project_id'),

            # These are standard attributes returned for the the AggregateCycleMetricsInterface

            func.max(work_item_delivery_cycles.c.end_date).label('latest_closed_date'),
            func.min(work_item_delivery_cycles.c.end_date).label('earliest_closed_date'),
            *[
                # This interpolates columns that calculate the specific
                # metrics that need to be returned based on the metrics specified in the cycle_metrics_trends_args
                *ProjectCycleMetricsTrends.get_metrics_columns(
                    cycle_metrics_trends_args, metrics_map, 'cycle_metrics'
                ),

                # This interpolates columns that calculate the specific
                # metrics that need to be returned based on the metrics specified in the cycle_metrics_trends_args
                *ProjectCycleMetricsTrends.get_metrics_columns(
                    cycle_metrics_trends_args, metrics_map, 'work_item_counts'
                )
            ]

        ]).select_from(
            project_nodes.join(
                work_items_sources, work_items_sources.c.project_id == project_nodes.c.id
            ).join(
                work_items,
                and_(
                    work_items.c.work_items_source_id == work_items_sources.c.id,
                    *ProjectCycleMetricsTrends.get_work_item_filter_clauses(cycle_metrics_trends_args)
                )
            ).outerjoin(
                # outer join here because we want to report timelines dates even
                # when there are no work items closed in that period.
                work_item_delivery_cycles,
                and_(
                    work_item_delivery_cycles.c.work_item_id == work_items.c.id,

                    cast(work_item_delivery_cycles.c.end_date, Date).between(
                        timeline_dates.c.measurement_date - timedelta(
                            days=measurement_window
                        ),
                        timeline_dates.c.measurement_date
                    ),
                    *ProjectCycleMetricsTrends.get_work_item_delivery_cycle_filter_clauses(
                        cycle_metrics_trends_args
                    )
                )
            )
        ).group_by(
            project_nodes.c.id
        ).lateral()

        return select([
            cycle_metrics.c.project_id.label('id'),
            func.json_agg(
                func.json_build_object(
                    'measurement_date', timeline_dates.c.measurement_date,
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
                        )
                    ],
                )

            ).label('cycle_metrics_trends')
        ]).select_from(
            timeline_dates.join(cycle_metrics, true())
        ).group_by(
            cycle_metrics.c.project_id
        )


class ProjectPipelineCycleMetricsTrends(ProjectCycleMetricsTrendsBase):
    interface = PipelineCycleMetricsTrends

    @classmethod
    def get_delivery_cycle_relation_for_pipeline(cls, cycle_metrics_trends_args, measurement_date, project_nodes):
        # This query provides a realation with a column interface similar to
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
            # the time from the start of the delivery cycle to the measurement date is the elapsed lead time for this
            # work item
            func.extract('epoch', measurement_date - func.min(work_item_delivery_cycles.c.start_date)).label(
                'lead_time'),
            func.max(work_item_delivery_cycle_durations.c.cumulative_time_in_state).label('backlog_time'),
            # This is the cycle time = lead_time - backlog time.
            (
                    func.extract('epoch', measurement_date - func.min(work_item_delivery_cycles.c.start_date)) -
                    # we are filtering out backlog work items items
                    # so the only items we can see here will have non null backlog time.
                    # also the only durations we are looking are the backlog durations, there
                    # can be multiple backlog states, so we could have a duration in each one,
                    # so taking the sum correctly gets you the current backlog duration.
                    func.sum(work_item_delivery_cycle_durations.c.cumulative_time_in_state)
            ).label('cycle_time')

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
                work_items_source_state_map.c.state_type == WorkItemsStateType.backlog.value,
                # add any other work item filters that the caller specifies.
                *ProjectCycleMetricsTrends.get_work_item_filter_clauses(cycle_metrics_trends_args),
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
    def get_current_pipeline_cycle_metrics_trends(cls, project_nodes, cycle_metrics_trends_args):

        measurement_date = datetime.utcnow()

        cycle_times = cls.get_delivery_cycle_relation_for_pipeline(
            cycle_metrics_trends_args, measurement_date, project_nodes)

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
                        )
                    ],
                )

            ).label('pipeline_cycle_metrics_trends')
        ]).select_from(
            cycle_metrics
        ).group_by(
            cycle_metrics.c.project_id
        )



    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        cycle_metrics_trends_args = kwargs.get('pipeline_cycle_metrics_trends_args')

        if cycle_metrics_trends_args.before is None:
            return ProjectPipelineCycleMetricsTrends.get_current_pipeline_cycle_metrics_trends(
                project_nodes,
                cycle_metrics_trends_args
            )
        else:
            raise ProcessingException("PipelineCyclemetricsTrends are not implemented for arbitrary dates (yet)")


class ProjectTraceabilityTrends(InterfaceResolver):
    interface = TraceabilityTrends

    # Total commit count calculates the overall universe of
    # commits over which traceability is calculated. This
    # the total number of commits in the window that are associated with all repositories
    # in this project. In general, spec_count + no_spec_count <= total_commit_count,
    # spec_count + nospec_count = total_commit_count only in the case that
    # all the commits come from repos that are exclusively shared with this project.
    # when there are shared repositories,  and if this
    # is a strict inequality then the difference is the commits that are associated with work items that
    # belong to some other project that shares the same repository.
    @staticmethod
    def subquery_total_commit_count(projects_timeline_dates, measurement_window):
        total_commit_count_lateral = select([
            projects_timeline_dates.c.id,
            projects_timeline_dates.c.measurement_date,
            func.coalesce(func.count(commits.c.id.distinct()), 0).label('total_commits')
        ]).select_from(
            projects_timeline_dates.outerjoin(
                projects_repositories, projects_repositories.c.project_id == projects_timeline_dates.c.id
            ).outerjoin(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            ).outerjoin(
                commits, commits.c.repository_id == repositories.c.id
            )
        ).where(
            commits.c.commit_date.between(
                projects_timeline_dates.c.measurement_date - timedelta(
                    days=measurement_window
                ),
                projects_timeline_dates.c.measurement_date
            )
        ).group_by(
            projects_timeline_dates.c.id,
            projects_timeline_dates.c.measurement_date
        ).lateral()

        # Do the lateral join - this calculate the timeline series for the metric
        return select([
            projects_timeline_dates.c.id,
            projects_timeline_dates.c.measurement_date,
            total_commit_count_lateral.c.total_commits
        ]).select_from(
            projects_timeline_dates.outerjoin(
                total_commit_count_lateral,
                and_(
                    projects_timeline_dates.c.id == total_commit_count_lateral.c.id,
                    projects_timeline_dates.c.measurement_date == total_commit_count_lateral.c.measurement_date
                )
            )
        ).alias()

    @staticmethod
    def subquery_spec_count(projects_timeline_dates, measurement_window):
        spec_count_lateral = select([
            projects_timeline_dates.c.id,
            projects_timeline_dates.c.measurement_date,
            func.coalesce(func.count(commits.c.id.distinct()), 0).label('spec_count')
        ]).select_from(
            projects_timeline_dates.outerjoin(
                work_items_sources, work_items_sources.c.project_id == projects_timeline_dates.c.id
            ).outerjoin(
                work_items, work_items.c.work_items_source_id == work_items_sources.c.id
            ).outerjoin(
                work_items_commits, work_items_commits.c.work_item_id == work_items.c.id
            ).outerjoin(
                commits, work_items_commits.c.commit_id == commits.c.id
            )
        ).where(
            commits.c.commit_date.between(
                projects_timeline_dates.c.measurement_date - timedelta(
                    days=measurement_window
                ),
                projects_timeline_dates.c.measurement_date
            )
        ).group_by(
            projects_timeline_dates.c.id,
            projects_timeline_dates.c.measurement_date
        ).lateral()

        return select([
            projects_timeline_dates.c.id,
            projects_timeline_dates.c.measurement_date,
            spec_count_lateral.c.spec_count
        ]).select_from(
            projects_timeline_dates.outerjoin(
                spec_count_lateral,
                and_(
                    projects_timeline_dates.c.id == spec_count_lateral.c.id,
                    projects_timeline_dates.c.measurement_date == spec_count_lateral.c.measurement_date
                )
            )
        ).alias()

    @staticmethod
    def subquery_nospec_count(projects_timeline_dates, measurement_window):
        # calculate the commits for that
        # are not associated with any work items at all.
        nospec_count_lateral = select([
            projects_timeline_dates.c.id,
            projects_timeline_dates.c.measurement_date,
            func.coalesce(func.count(commits.c.id.distinct()), 0).label('nospec_count')
        ]).select_from(
            projects_timeline_dates.outerjoin(
                projects_repositories, projects_repositories.c.project_id == projects_timeline_dates.c.id
            ).outerjoin(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            ).outerjoin(
                commits,
                and_(
                    commits.c.repository_id == repositories.c.id,

                )
            ).outerjoin(
                work_items_commits, work_items_commits.c.commit_id == commits.c.id
            )
        ).where(
            and_(
                work_items_commits.c.work_item_id == None,
                commits.c.commit_date.between(
                    projects_timeline_dates.c.measurement_date - timedelta(
                        days=measurement_window
                    ),
                    projects_timeline_dates.c.measurement_date
                )
            )
        ).group_by(
            projects_timeline_dates.c.id,
            projects_timeline_dates.c.measurement_date
        ).lateral()

        return select([
            projects_timeline_dates.c.id,
            projects_timeline_dates.c.measurement_date,
            nospec_count_lateral.c.nospec_count
        ]).select_from(
            projects_timeline_dates.outerjoin(
                nospec_count_lateral,
                and_(
                    projects_timeline_dates.c.id == nospec_count_lateral.c.id,
                    projects_timeline_dates.c.measurement_date == nospec_count_lateral.c.measurement_date
                )
            )
        ).alias()

    @staticmethod
    def interface_selector(project_nodes, **kwargs):
        traceability_trends_args = kwargs.get('traceability_trends_args')
        measurement_window = traceability_trends_args.measurement_window

        # Get the a list of dates for trending using the trends_args for control
        timeline_dates = get_timeline_dates_for_trending(
            traceability_trends_args,
            arg_name='traceability_trends',
            interface_name='TraceabilityTrends'
        )

        if measurement_window is None:
            raise ProcessingException(
                "'measurement_window' must be specified when calculating ProjectCycleMetricsTrends"
            )

        projects_timeline_dates = select([project_nodes, timeline_dates]).alias()

        # Calculate the trendlines for the total commits in each project for each
        # date in the time series
        total_commit_count = ProjectTraceabilityTrends.subquery_total_commit_count(
            projects_timeline_dates,
            measurement_window
        )

        # Calculate the trendlines for number of specs in each project for each
        # date in the time series.
        spec_count = ProjectTraceabilityTrends.subquery_spec_count(
            projects_timeline_dates,
            measurement_window
        )

        # Calculate the trendlines for number of commits in each project that are not associated
        # with any specs, for each date in the time series.
        nospec_count = ProjectTraceabilityTrends.subquery_nospec_count(
            projects_timeline_dates,
            measurement_window
        )

        # Now put them together to do the actual traceability calc and assemble the final JSON result
        result = select([
            total_commit_count.c.id,
            func.json_agg(
                func.json_build_object(
                    'measurement_date', total_commit_count.c.measurement_date,
                    'measurement_window', measurement_window,
                    'spec_count', func.coalesce(spec_count.c.spec_count, 0),
                    'nospec_count', func.coalesce(nospec_count.c.nospec_count, 0),
                    'total_commits', func.coalesce(total_commit_count.c.total_commits, 0),
                    'traceability',
                    # we calculate traceability as a ratio of the commits associated with work items
                    # in our project relative to the universe of commits that includes this set and the set
                    # associated with no work items. Thus if there are many projects that share a repository with a
                    # a lot of nospec commits, it will affect the traceability of ALL of the projects that share this
                    # repo.
                    case([
                        (
                            func.coalesce(nospec_count.c.nospec_count, 0) + func.coalesce(
                                spec_count.c.spec_count) != 0,
                            func.coalesce(spec_count.c.spec_count, 0) / (1.0 * (
                                    func.coalesce(spec_count.c.spec_count, 0) + func.coalesce(
                                nospec_count.c.nospec_count, 0)))
                        )
                    ], else_=0),
                )
            ).label('traceability_trends')
        ]).select_from(
            total_commit_count.outerjoin(
                nospec_count,
                and_(
                    total_commit_count.c.id == nospec_count.c.id,
                    total_commit_count.c.measurement_date == nospec_count.c.measurement_date
                )
            ).outerjoin(
                spec_count,
                and_(
                    total_commit_count.c.id == spec_count.c.id,
                    total_commit_count.c.measurement_date == spec_count.c.measurement_date
                )
            )
        ).group_by(
            total_commit_count.c.id
        )

        return result
