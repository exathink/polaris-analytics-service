# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import select, bindparam, and_

from polaris.analytics.db.model import \
    work_items, work_item_state_transitions, \
    work_items_commits, repositories, commits

from polaris.analytics.service.graphql.interfaces import WorkItemInfo, WorkItemStateTransition, CommitInfo

from .sql_expressions import work_item_info_columns, work_item_event_columns

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
    interfaces = (WorkItemInfo, WorkItemStateTransition)

    @staticmethod
    def selectable(**kwargs):
        return select([
            *work_item_info_columns(work_items),
            *work_item_event_columns(work_item_state_transitions)
        ]).where(
            and_(
                work_item_state_transitions.c.work_item_id == work_items.c.id,
                work_items.c.key == bindparam('key')
            )
        )


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


