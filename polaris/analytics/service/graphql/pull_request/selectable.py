# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2019) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from sqlalchemy import select, bindparam
from polaris.analytics.db.model import pull_requests
from polaris.graphql.interfaces import NamedNode
from polaris.graphql.base_classes import NamedNodeResolver


class PullRequestNode(NamedNodeResolver):
    interfaces = (NamedNode,)

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            pull_requests.c.id,
            pull_requests.c.key,
            pull_requests.c.title.label('name')
        ]).select_from(
            pull_requests
        ).where(
            pull_requests.c.key == bindparam('key')
        )
