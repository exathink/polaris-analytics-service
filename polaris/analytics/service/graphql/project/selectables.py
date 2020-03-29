# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

from datetime import datetime, timedelta

# Author: Krishna Kumar
from sqlalchemy import select, func, bindparam, distinct, and_, cast, Text, between, extract, case, literal_column
from polaris.analytics.db.enums import WorkItemsStateType

from polaris.analytics.db.model import projects, projects_repositories, organizations, \
    repositories, contributors, \
    contributor_aliases, repositories_contributor_aliases, commits, work_items_sources, \
    work_items, work_item_state_transitions, work_items_commits, work_item_delivery_cycles, \
    work_item_delivery_cycle_durations, work_items_source_state_map
from polaris.graphql.base_classes import NamedNodeResolver, InterfaceResolver, ConnectionResolver, \
    SelectableFieldResolver
from polaris.graphql.interfaces import NamedNode
from polaris.graphql.utils import nulls_to_zero
from polaris.utils.datetime_utils import time_window
from ..commit.sql_expressions import commit_info_columns, commits_connection_apply_time_window_filters
from ..contributor.sql_expressions import contributor_count_apply_contributor_days_filter
from ..interfaces import \
    CommitSummary, ContributorCount, RepositoryCount, OrganizationRef, CommitCount, \
    CumulativeCommitCount, CommitInfo, WeeklyContributorCount, ArchivedStatus, \
    WorkItemEventSpan, WorkItemsSourceRef, WorkItemInfo, WorkItemStateTransition, WorkItemCommitInfo, \
    WorkItemStateTypeCounts, AggregateCycleMetrics
from ..work_item.sql_expressions import work_item_events_connection_apply_time_window_filters, work_item_event_columns, \
    work_item_info_columns, work_item_commit_info_columns, work_items_connection_apply_time_window_filters

from ..work_item import sql_expressions

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
        select_stmt = select([
            *commit_info_columns(repositories, commits)
        ]).select_from(
            projects.join(
                projects_repositories, projects_repositories.c.project_id == projects.c.id
            ).join(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            ).join(commits)
        ).where(
            projects.c.key == bindparam('key')
        )
        return commits_connection_apply_time_window_filters(select_stmt, commits, **kwargs)

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
        return work_items_connection_apply_time_window_filters(select_stmt, work_items, **kwargs)

    @staticmethod
    def sort_order(project_work_items_nodes, **kwargs):
        return [project_work_items_nodes.c.updated_at.desc()]


class ProjectWorkItemEventNodes(ConnectionResolver):
    interfaces = (NamedNode, WorkItemInfo, WorkItemStateTransition, WorkItemsSourceRef)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        select_stmt = select([
            work_items_sources.c.key.label('work_items_source_key'),
            work_items_sources.c.name.label('work_items_source_name'),
            *work_item_event_columns(work_items, work_item_state_transitions)
        ]).select_from(
            projects.join(
                work_items_sources, work_items_sources.c.project_id == projects.c.id
            ).join(
                work_items
            ).join(
                work_item_state_transitions
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
        state_types_count = select([
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
        ).group_by(
            project_nodes.c.id,
            work_items.c.state_type
        ).alias()

        return select([
            project_nodes.c.id,
            func.json_agg(
                func.json_build_object(
                    'state_type', state_types_count.c.state_type,
                    'count', state_types_count.c.count
                )
            ).label('work_item_state_type_counts')

        ]).select_from(
            project_nodes.outerjoin(
                state_types_count, project_nodes.c.id == state_types_count.c.id
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
            kwargs.get('cycle_metrics_days')
        ).alias()

        project_work_item_cycle_metrics = select([
            project_nodes.c.id.label('project_id'),
            work_items_cycle_metrics.c.id.label('work_item_id'),
            work_items_cycle_metrics.c.lead_time,
            work_items_cycle_metrics.c.cycle_time,
            work_items_cycle_metrics.c.end_date,
        ]).select_from(
            project_nodes.join(
                work_items_sources, work_items_sources.c.project_id == project_nodes.c.id
            ).join(
                work_items_cycle_metrics, work_items_cycle_metrics.c.work_items_source_id == work_items_sources.c.id
            )).alias()

        return select([
            project_nodes.c.id,
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
            func.count(project_work_item_cycle_metrics.c.work_item_id).label('work_items_in_scope'),
            func.sum(
                case([
                    (and_(project_work_item_cycle_metrics.c.work_item_id != None,
                          project_work_item_cycle_metrics.c.cycle_time == None), 1)
                ], else_=0)
            ).label('work_items_with_null_cycle_time'),
            literal_column(f'{target_percentile}').label('target_percentile')
        ]).select_from(
            project_nodes.outerjoin(
                project_work_item_cycle_metrics, project_nodes.c.id == project_work_item_cycle_metrics.c.project_id
            )
        ).group_by(
            project_nodes.c.id
        )
