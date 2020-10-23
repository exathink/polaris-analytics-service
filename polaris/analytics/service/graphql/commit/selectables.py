# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from ..interfaces import CommitInfo, FileTypesSummary, WorkItemsSummaries
from sqlalchemy import select, func, bindparam, and_, case
from polaris.analytics.db.model import commits, repositories, work_items_commits, work_items
from polaris.graphql.base_classes import NamedNodeResolver, InterfaceResolver
from .sql_expressions import commit_info_columns


class CommitNode(NamedNodeResolver):
    interface = CommitInfo

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            *commit_info_columns(repositories, commits)
        ]).select_from(
            repositories.join(
                commits
            )
        ).where(
            and_(
                repositories.c.key == bindparam('repository_key'),
                commits.c.source_commit_id == bindparam('commit_key')
            )
        )


class CommitFileTypesSummary:
    interface = FileTypesSummary

    @staticmethod
    def selectable(**kwargs):
        return select([commits.c.source_file_types_summary]).where(
            and_(
                repositories.c.key == bindparam('repository_key'),
                commits.c.source_commit_id == bindparam('commit_key')
            )
        )


class CommitsWorkItemsSummaries(InterfaceResolver):
    interface = WorkItemsSummaries

    @staticmethod
    def interface_selector(commit_nodes, **kwargs):
        return select([
            commit_nodes.c.id,
            func.json_agg(
                case([
                    (
                        work_items_commits.c.work_item_id != None,
                        func.json_build_object(
                            'key', work_items.c.key,
                            'name', work_items.c.name,
                            'display_id', work_items.c.display_id,
                            'url', work_items.c.url,
                            'work_item_type', work_items.c.work_item_type,
                            'state_type', work_items.c.state_type,
                            'state', work_items.c.state
                        )
                    )
                ], else_=None)
            ).label('work_items_summaries')
        ]).select_from(
            commit_nodes.outerjoin(
                work_items_commits, work_items_commits.c.commit_id == commit_nodes.c.id
            ).outerjoin(
                work_items, work_items_commits.c.work_item_id == work_items.c.id
            )
        ).group_by(
            commit_nodes.c.id
        )
