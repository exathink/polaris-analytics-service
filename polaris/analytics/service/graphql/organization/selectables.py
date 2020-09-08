# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from datetime import datetime, timedelta
from polaris.utils.datetime_utils import time_window

from sqlalchemy import select, func, bindparam, distinct, and_, between, cast, Text, extract

from polaris.graphql.utils import nulls_to_zero
from polaris.graphql.interfaces import NamedNode
from polaris.graphql.base_classes import NamedNodeResolver, ConnectionResolver, InterfaceResolver, SelectableFieldResolver

from polaris.analytics.db.model import \
    organizations, projects, projects_repositories, \
    repositories, contributors, commits, \
    repositories_contributor_aliases, contributor_aliases, \
    work_items_sources, work_items, work_item_state_transitions, work_items_commits, \
    work_items_source_state_map
 
from ..interfaces import CommitSummary, CommitCount, ContributorCount, \
    ProjectCount, RepositoryCount, WorkItemsSourceCount,  \
    WeeklyContributorCount, CommitInfo, WorkItemInfo, WorkItemsSourceRef,\
    WorkItemStateTransition, WorkItemCommitInfo, WorkItemEventSpan, ArchivedStatus

from ..commit.sql_expressions import commit_info_columns, commits_connection_apply_filters
from ..work_item.sql_expressions import \
    work_item_info_columns, \
    work_item_event_columns, \
    work_item_commit_info_columns, \
    work_items_connection_apply_filters, \
    work_item_events_connection_apply_time_window_filters


class OrganizationNode(NamedNodeResolver):
    interface = NamedNode

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            organizations.c.id,
            organizations.c.key.label('key'),
            organizations.c.name,

        ]).select_from(
            organizations
        ).where(organizations.c.key == bindparam('key'))


# ----------------------------------------------------------------------------------------------------------------------
# Connection Resolvers

class OrganizationProjectsNodes(ConnectionResolver):
    interfaces = (NamedNode, ArchivedStatus)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        return select([
            projects.c.id,
            projects.c.key.label('key'),
            projects.c.name,
            projects.c.archived
        ]).select_from(
            projects.join(
                organizations
            )
        ).where(organizations.c.key == bindparam('key'))


class OrganizationRecentlyActiveProjectsNodes(ConnectionResolver):
    interfaces = (NamedNode, CommitCount)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        end_date = kwargs.get('before') or datetime.utcnow()
        window = time_window(begin=end_date - timedelta(days=kwargs.get('days', 7)), end=end_date)

        return select([
            projects.c.id,
            func.min(cast(projects.c.key, Text)).label('key'),
            func.min(projects.c.name).label('name'),
            func.count(commits.c.id).label('commit_count')
        ]).select_from(
            organizations.join(
                projects, projects.c.organization_id == organizations.c.id
            ).join(
                projects_repositories, projects_repositories.c.project_id == projects.c.id
            ).join(
                repositories, projects_repositories.c.repository_id == repositories.c.id
            ).join(
                commits
            )
        ).where(
            and_(
                organizations.c.key == bindparam('key'),
                between(commits.c.commit_date, window.begin, window.end)
            )
        ).group_by(
            projects.c.id
        )

    @staticmethod
    def sort_order(organizations_recently_active_projects, **kwargs):
        return [organizations_recently_active_projects.c.commit_count.desc()]


class OrganizationRepositoriesNodes(ConnectionResolver):
    interface = NamedNode

    @staticmethod
    def connection_nodes_selector(**kwargs):
        return select([
            repositories.c.id,
            repositories.c.key,
            repositories.c.name,
            repositories.c.description
        ]).select_from(
            repositories.join(
                organizations
            )
        ).where(organizations.c.key == bindparam('key'))


class OrganizationRecentlyActiveRepositoriesNodes(ConnectionResolver):
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
            organizations.join(
                repositories.join(
                    commits
                )
            )
        ).where(
            and_(
                organizations.c.key == bindparam('key'),
                between(commits.c.commit_date, window.begin, window.end)
            )
        ).group_by(
            repositories.c.id
        )

    @staticmethod
    def sort_order(organizations_recently_active_repositories, **kwargs):
        return [organizations_recently_active_repositories.c.commit_count.desc()]


class OrganizationContributorNodes(ConnectionResolver):
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
                        organizations
                    )
                )
            )
        ).where(
            and_(
                organizations.c.key == bindparam('key'),
                repositories_contributor_aliases.c.robot == False
            )
        ).distinct()


class OrganizationCommitNodes(ConnectionResolver):
    interface = CommitInfo

    @staticmethod
    def connection_nodes_selector(**kwargs):
        select_stmt = select([
            *commit_info_columns(repositories, commits)
        ]).select_from(
            organizations.join(
                repositories, repositories.c.organization_id == organizations.c.id
            ).join(
                commits
            )
        ).where(
            organizations.c.key == bindparam('key')
        )
        return commits_connection_apply_filters(select_stmt, commits, **kwargs)

    @staticmethod
    def sort_order(repository_commit_nodes, **kwargs):
        return [repository_commit_nodes.c.commit_date.desc()]


class OrganizationWorkItemNodes(ConnectionResolver):
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
            organizations.join(
                work_items_sources, work_items_sources.c.organization_id == organizations.c.id
            ).join(
                work_items
            )
        ).where(
            organizations.c.key == bindparam('key')
        )
        return work_items_connection_apply_filters(select_stmt, work_items, **kwargs)

    @staticmethod
    def sort_order(organization_work_items_nodes, **kwargs):
        return [organization_work_items_nodes.c.updated_at.desc()]


class OrganizationWorkItemEventNodes(ConnectionResolver):
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
            organizations.join(
                work_items_sources, work_items_sources.c.organization_id == organizations.c.id
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
            organizations.c.key == bindparam('key')
        )
        return work_item_events_connection_apply_time_window_filters(select_stmt, work_item_state_transitions, **kwargs)

    @staticmethod
    def sort_order(organization_work_item_event_nodes, **kwargs):
        return [organization_work_item_event_nodes.c.event_date.desc()]


class OrganizationWorkItemCommitNodes(ConnectionResolver):
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
            organizations.join(
                work_items_sources, work_items_sources.c.organization_id == organizations.c.id
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
            organizations.c.key == bindparam('key')
        )
        return commits_connection_apply_filters(select_stmt, commits, **kwargs)

    @staticmethod
    def sort_order(organization_work_item_commits_nodes, **kwargs):
        return [organization_work_item_commits_nodes.c.commit_date.desc()]


class OrganizationRecentlyActiveContributorNodes(ConnectionResolver):
    interfaces = (NamedNode, CommitCount)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        end_date = kwargs.get('before') or datetime.utcnow()
        window = time_window(begin=end_date - timedelta(days=kwargs.get('days', 7)), end=end_date)

        return select([
            commits.c.author_contributor_key.label('key'),
            func.min(commits.c.author_contributor_name).label('name'),
            func.count(commits.c.id).label('commit_count')

        ]).select_from(
            organizations.join(
                repositories
            ).join(
                commits.join(
                    contributor_aliases, commits.c.author_contributor_alias_id == contributor_aliases.c.id
                )
            )
        ).where(
            and_(
                organizations.c.key == bindparam('key'),
                between(commits.c.commit_date, window.begin, window.end),
                contributor_aliases.c.robot == False
            )
        ).group_by(
            commits.c.author_contributor_key
        )

    @staticmethod
    def sort_order(recently_active_contributors, **kwargs):
        return [recently_active_contributors.c.commit_count.desc()]

# ----------------------------------------------------------------------------------------------------------------------
#  Interface Resolvers

class OrganizationsCommitSummary(InterfaceResolver):
    interface = CommitSummary

    @staticmethod
    def interface_selector(organization_nodes, **kwargs):
        return select([
            organization_nodes.c.id,
            func.sum(repositories.c.commit_count).label('commit_count'),
            func.min(repositories.c.earliest_commit).label('earliest_commit'),
            func.max(repositories.c.latest_commit).label('latest_commit')

        ]).select_from(
            organization_nodes.outerjoin(repositories, organization_nodes.c.id == repositories.c.organization_id)
        ).group_by(organization_nodes.c.id)

    @staticmethod
    def sort_order(organizations_commit_summary, **kwargs):
        return [nulls_to_zero(organizations_commit_summary.c.commit_count).desc()]

class OrganizationsContributorCount(InterfaceResolver):
    interface = ContributorCount

    @staticmethod
    def interface_selector(organization_nodes, **kwargs):
        return select([
            organization_nodes.c.id,
            func.count(distinct(repositories_contributor_aliases.c.contributor_id)).label('contributor_count')
        ]).select_from(
            organization_nodes.outerjoin(
                repositories, repositories.c.organization_id == organization_nodes.c.id
            ).outerjoin(
                repositories_contributor_aliases, repositories.c.id == repositories_contributor_aliases.c.repository_id
            )
        ).where(
            repositories_contributor_aliases.c.robot == False
        ).group_by(organization_nodes.c.id)


class OrganizationsProjectCount(InterfaceResolver):
    interface = ProjectCount

    @staticmethod
    def interface_selector(organization_nodes, **kwargs):
        return select([
            organization_nodes.c.id,
            func.count(projects.c.id).label('project_count')
        ]).select_from(
            organization_nodes.outerjoin(
                projects, projects.c.organization_id == organization_nodes.c.id
            )
        ).group_by(organization_nodes.c.id)


class OrganizationsRepositoryCount(InterfaceResolver):
    interface = RepositoryCount

    @staticmethod
    def interface_selector(organization_nodes, **kwargs):
        return select([
            organization_nodes.c.id,
            func.count(repositories.c.id).label('repository_count')
        ]).select_from(
            organization_nodes.outerjoin(
                repositories, repositories.c.organization_id == organization_nodes.c.id
            )
        ).group_by(organization_nodes.c.id)


class OrganizationsWorkItemsSourceCount(InterfaceResolver):
    interface = WorkItemsSourceCount

    @staticmethod
    def interface_selector(organization_nodes, **kwargs):
        return select([
            organization_nodes.c.id,
            func.count(work_items_sources.c.id).label('work_items_source_count')
        ]).select_from(
            organization_nodes.outerjoin(
                work_items_sources, work_items_sources.c.organization_id == organization_nodes.c.id
            )
        ).group_by(organization_nodes.c.id)


class OrganizationWorkItemEventSpan(InterfaceResolver):
    interface = WorkItemEventSpan

    @staticmethod
    def interface_selector(organization_nodes, **kwargs):
        return select([
            organization_nodes.c.id,
            func.min(work_items.c.created_at).label('earliest_work_item_event'),
            func.max(work_items.c.updated_at).label('latest_work_item_event')
        ]).select_from(
            organization_nodes.outerjoin(
                work_items_sources, work_items_sources.c.organization_id == organization_nodes.c.id
            ).outerjoin(
                work_items, work_items.c.work_items_source_id == work_items_sources.c.id
            )
        ).group_by(organization_nodes.c.id)

# ----------------------------------------------------------------------------------------------------------------------
# Selectable field resolvers

class OrganizationWeeklyContributorCount(SelectableFieldResolver):

    interface = WeeklyContributorCount

    @staticmethod
    def selectable(**kwargs):
        return select([
            extract('year', commits.c.commit_date).label('year'),
            extract('week', commits.c.commit_date).label('week'),
            func.count(distinct(commits.c.author_contributor_key)).label('contributor_count')
        ]).select_from(
            organizations.join(
                repositories
            ).join(
                commits, commits.c.repository_id == repositories.c.id
            )
        ).where(
            organizations.c.key == bindparam('key')
        ).group_by(
            extract('year', commits.c.commit_date),
            extract('week', commits.c.commit_date)
        )