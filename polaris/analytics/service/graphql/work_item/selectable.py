# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import select, bindparam, and_

from polaris.analytics.db.model import work_items, work_item_state_transitions
from polaris.analytics.service.graphql.interfaces import WorkItemInfo, WorkItemStateTransition
from .sql_expressions import work_item_info_columns, work_item_event_columns


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


