# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2019) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from datetime import datetime
from sqlalchemy import select, bindparam, func, case
from polaris.analytics.db.model import pull_requests, repositories
from polaris.graphql.interfaces import NamedNode
from polaris.graphql.base_classes import NamedNodeResolver, InterfaceResolver
from polaris.analytics.service.graphql.interfaces import PullRequestInfo, BranchRef
from .sql_expressions import pull_request_info_columns


class PullRequestNode(NamedNodeResolver):
    interfaces = (NamedNode, PullRequestInfo)

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            *pull_request_info_columns(pull_requests),
            (func.extract('epoch',
                          case(
                              [
                                  (pull_requests.c.state != 'open',
                                   (pull_requests.c.updated_at - pull_requests.c.created_at))
                              ],
                              else_=(datetime.utcnow() - pull_requests.c.created_at)
                          )

                          ) / (1.0 * 3600 * 24)).label('age')
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
