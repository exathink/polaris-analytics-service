# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import select, bindparam, and_, func

from polaris.graphql.base_classes import NamedNodeResolver, InterfaceResolver, ConnectionResolver

from polaris.analytics.db.model import \
    work_items, work_item_state_transitions, \
    work_items_commits, repositories, commits, \
    work_items_sources

from polaris.analytics.service.graphql.interfaces import \
    NamedNode, WorkItemInfo, WorkItemCommitInfo, \
    WorkItemsSourceRef, WorkItemStateTransition, CommitInfo, CommitSummary, CycleMetrics

from .sql_expressions import work_item_info_columns, work_item_event_columns, work_item_commit_info_columns, \
    work_item_events_connection_apply_time_window_filters

from ..commit.sql_expressions import commit_info_columns, commits_connection_apply_time_window_filters, \
    commit_key_column, commit_name_column


class WorkItemNode(NamedNodeResolver):
    interfaces = (NamedNode, WorkItemInfo)

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            work_items.c.id,
            work_items.c.key,
            work_items.c.name,
            *work_item_info_columns(work_items)
        ]).where(
            work_items.c.key == bindparam('key')
        )


# Commits collection on a single work item instance
class WorkItemCommitNodes(ConnectionResolver):
    interface = CommitInfo

    @staticmethod
    def connection_nodes_selector(**kwargs):
        select_stmt = select([
            *commit_info_columns(repositories, commits)
        ]).select_from(
            work_items.join(
                work_items_commits
            ).join(
                commits
            ).join(
                repositories
            )
        ).where(
            work_items.c.key == bindparam('key')
        )
        return commits_connection_apply_time_window_filters(select_stmt, commits, **kwargs)

    @staticmethod
    def sort_order(work_item_commit_nodes, **kwargs):
        return [work_item_commit_nodes.c.commit_date.desc()]


# work item events collection on a single work item

class WorkItemEventNodes(ConnectionResolver):
    interfaces = (NamedNode, WorkItemsSourceRef, WorkItemInfo, WorkItemStateTransition)

    @staticmethod
    def connection_nodes_selector(**kwargs):
        select_stmt = select([
            work_items_sources.c.key.label('work_items_source_key'),
            work_items_sources.c.name.label('work_items_source_name'),
            *work_item_event_columns(work_items, work_item_state_transitions)
        ]).where(
            and_(
                work_items_sources.c.id == work_items.c.work_items_source_id,
                work_item_state_transitions.c.work_item_id == work_items.c.id,
                work_items.c.key == bindparam('key')
            )
        )

        return work_item_events_connection_apply_time_window_filters(select_stmt, work_item_state_transitions, **kwargs)


# a generic work item event accessed via its node id of the form work_item_key:seq_no

class WorkItemEventNode(NamedNodeResolver):
    interfaces = (NamedNode, WorkItemInfo, WorkItemStateTransition, WorkItemsSourceRef)

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            *work_item_event_columns(work_items, work_item_state_transitions),
            work_items_sources.c.key.label('work_items_source_key'),
            work_items_sources.c.name.label('work_items_source_name'),

        ]).select_from(
            work_items.join(
                work_item_state_transitions, work_item_state_transitions.c.work_item_id == work_items.c.id
            ).join(
                work_items_sources, work_items_sources.c.id == work_items.c.work_items_source_id
            )
        ).where(
            and_(
                work_items.c.key == bindparam('work_item_key'),
                work_item_state_transitions.c.seq_no == bindparam('seq_no')
            )
        )


# a generic work_item_commit accessed by its node id of the form work_item_key:commit_key
class WorkItemCommitNode(NamedNodeResolver):
    interface = (WorkItemInfo, WorkItemCommitInfo)

    @staticmethod
    def named_node_selector(**kwargs):
        select_stmt = select([
            *work_item_info_columns(work_items),
            *work_item_commit_info_columns(work_items, repositories, commits)
        ]).select_from(
            work_items.join(
                work_items_commits
            ).join(
                commits
            ).join(
                repositories
            )
        ).where(
            and_(
                work_items.c.key == bindparam('work_item_key'),
                commits.c.key == bindparam('commit_key')
            )
        )


class WorkItemsCommitSummary(InterfaceResolver):
    interface = CommitSummary

    @staticmethod
    def interface_selector(work_item_nodes, **kwargs):
        return select([
            work_item_nodes.c.id,
            func.count(commits.c.commit_date).label('commit_count'),
            func.min(commits.c.commit_date).label('earliest_commit'),
            func.max(commits.c.commit_date).label('latest_commit')

        ]).select_from(
            work_item_nodes.join(
                work_items_commits, work_items_commits.c.work_item_id == work_item_nodes.c.id
            ).join(
                commits, commits.c.id == work_items_commits.c.commit_id
            )
        ).group_by(work_item_nodes.c.id)


class WorkItemsCycleMetrics(InterfaceResolver):
    interface = CycleMetrics

    @staticmethod
    def interface_selector(named_node_cte, **kwargs):
        pass

