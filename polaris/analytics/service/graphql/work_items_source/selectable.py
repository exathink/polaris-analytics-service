# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import select, bindparam, and_

from polaris.analytics.service.graphql.interfaces import WorkItemInfo, WorkItemsSourceRef
from polaris.graphql.interfaces import NamedNode

from polaris.analytics.db.model import work_items_sources, work_items
from ..work_item.sql_expressions import work_item_info_columns, work_items_connection_apply_time_window_filters


class WorkItemsSourceNode:
    interfaces = (NamedNode, )

    @staticmethod
    def named_node_selector(**kwargs):
        return select([
            work_items_sources.c.id,
            work_items_sources.c.key.label('key'),
            work_items_sources.c.name,

        ]).select_from(
            work_items_sources
        ).where(
            work_items_sources.c.key == bindparam('key')
        )

class WorkItemsSourceWorkItemNodes:
    interfaces = (NamedNode, WorkItemInfo, WorkItemsSourceRef)

    @staticmethod
    def selectable(**kwargs):
        select_stmt = select([
            work_items_sources.c.key.label('work_items_source_key'),
            work_items_sources.c.name.label('work_items_source_name'),
            work_items.c.name,
            work_items.c.key,
            *work_item_info_columns(work_items)
        ]).select_from(
            work_items.join(
                work_items_sources, work_items_sources.c.id == work_items.c.work_items_source_id
            )
        ).where(
            work_items_sources.c.key == bindparam('key')
        )
        return work_items_connection_apply_time_window_filters(select_stmt, work_items, **kwargs)







