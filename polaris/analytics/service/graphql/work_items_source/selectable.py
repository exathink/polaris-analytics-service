# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import select, bindparam, and_
from polaris.graphql.interfaces import NamedNode

from polaris.analytics.db.model import work_items_sources


class WorkItemsSourceNode:
    interfaces = (NamedNode, )

    @staticmethod
    def selectable(**kwargs):
        return select([
            work_items_sources.c.id,
            work_items_sources.c.key,
            work_items_sources.c.name,

        ]).where(
            work_items_sources.c.key == bindparam('key')
        )







