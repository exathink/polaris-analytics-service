# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import select, bindparam, and_

from polaris.analytics.db.model import \
    work_items, work_item_state_transitions, \
    work_items_commits, repositories, commits, \
    work_items_sources

from polaris.analytics.service.graphql.interfaces import WorkItemInfo, WorkItemsSourceRef, WorkItemStateTransition, CommitInfo

from .sql_expressions import work_item_info_columns, work_item_event_columns, work_item_events_connection_apply_time_window_filters

from ..commit.sql_expressions import commit_info_columns, commits_connection_apply_time_window_filters


class WorkItemNode:
    interface = WorkItemInfo

    @staticmethod
    def selectable(**kwargs):
        return select([
            *work_item_info_columns(work_items)
        ]).where(
            work_items.c.key == bindparam('key')
        )


class WorkItemEventNodes:
    interfaces = (WorkItemsSourceRef, WorkItemInfo, WorkItemStateTransition)

    @staticmethod
    def selectable(**kwargs):
        select_stmt =  select([
            work_items_sources.c.key.label('work_items_source_key'),
            work_items_sources.c.name.label('work_items_source_name'),
            *work_item_info_columns(work_items),
            *work_item_event_columns(work_item_state_transitions)
        ]).where(
            and_(
                work_items_sources.c.id == work_items.c.work_items_source_id,
                work_item_state_transitions.c.work_item_id == work_items.c.id,
                work_items.c.key == bindparam('key')
            )
        )

        return work_item_events_connection_apply_time_window_filters(select_stmt, work_item_state_transitions, **kwargs)


class WorkItemCommitNodes:
    interface = CommitInfo

    @staticmethod
    def selectable(**kwargs):
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
    def sort_order(repository_commit_nodes, **kwargs):
        return [repository_commit_nodes.c.commit_date.desc()]


