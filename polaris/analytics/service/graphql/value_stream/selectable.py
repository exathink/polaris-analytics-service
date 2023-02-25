# -*- coding: utf-8 -*-

#  Copyright (c) Exathink, LLC  2016-2023.
#  All rights reserved
#

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import select, bindparam, func

from polaris.analytics.db.model import value_streams


from polaris.graphql.interfaces import KeyIdNode, NamedNode
from polaris.graphql.base_classes import NamedNodeResolver
from ..interfaces import UserInfo, UserRoles


class ValueStreamNode(NamedNodeResolver):
    interfaces = (NamedNode,)

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            value_streams.c.id,
            value_streams.c.key,
            value_streams.c.name,
            value_streams.c.work_item_selectors
        ]).where(
            value_streams.c.key == bindparam('key')
        )