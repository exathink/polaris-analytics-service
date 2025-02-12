# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2019) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from datetime import datetime
from sqlalchemy import select, bindparam, func, case
from polaris.analytics.db.model import pull_requests, repositories, work_items_pull_requests, work_items
from polaris.graphql.interfaces import NamedNode
from polaris.graphql.base_classes import NamedNodeResolver, InterfaceResolver
from polaris.analytics.service.graphql.interfaces import PullRequestInfo, BranchRef, WorkItemsSummaries
from .sql_expressions import pull_request_info_columns


class PullRequestNode(NamedNodeResolver):
    interfaces = (NamedNode, PullRequestInfo)

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            *pull_request_info_columns(pull_requests)
        ]).select_from(
            pull_requests
        ).where(
            pull_requests.c.key == bindparam('key')
        )


class PullRequestBranchRef(InterfaceResolver):
    interface = BranchRef

    @staticmethod
    def interface_selector(pull_request_nodes, **kwargs):
        # FIXME: Should pull_request_nodes be used here or we can just directly \
        #  use pull_requests avoiding a join of both
        return select([
            pull_request_nodes.c.id,
            repositories.c.name.label('repository_name'),
            repositories.c.key.label('repository_key'),
            pull_requests.c.source_branch.label('branch_name')
        ]).select_from(
            pull_request_nodes.outerjoin(
                pull_requests, pull_requests.c.id == pull_request_nodes.c.id
            ).join(
                repositories, repositories.c.id == pull_requests.c.repository_id
            )
        )


class PullRequestWorkItemsSummaries(InterfaceResolver):
    interface = WorkItemsSummaries

    @staticmethod
    def interface_selector(pull_request_nodes, **kwargs):
        return select([
            pull_request_nodes.c.id,
            func.json_agg(
                case([
                    (
                        work_items_pull_requests.c.work_item_id != None,
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
            pull_request_nodes.outerjoin(
                work_items_pull_requests, work_items_pull_requests.c.pull_request_id == pull_request_nodes.c.id
            ).outerjoin(
                work_items, work_items.c.id == work_items_pull_requests.c.work_item_id
            )
        ).group_by(
            pull_request_nodes.c.id
        )
