# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import select, bindparam, and_
from polaris.graphql.interfaces import NamedNode
from polaris.analytics.service.graphql.interfaces import WorkItemInfo, WorkItemStateTransitions
from polaris.analytics.db.model import work_items, work_item_state_transitions
from .sql_expressions import work_item_info_columns

class WorkItemNode:
    interface = WorkItemInfo

    @staticmethod
    def selectable(**kwargs):
        return select([
            *work_item_info_columns(work_items)
        ]).where(
            work_items.c.key == bindparam('key')
        )


class WorkItemStateTransitionNodes:
    interfaces = (NamedNode, WorkItemStateTransitions)

    @staticmethod
    def selectable(**kwargs):
        return select([
            work_items.c.id,
            work_items.c.name,
            work_items.c.key,
            work_item_state_transitions.c.created_at.label('event_date'),
            work_item_state_transitions.c.seq_no,
            work_item_state_transitions.c.previous_state,
            work_item_state_transitions.c.state.label('new_state'),
        ]).where(
            and_(
                work_items.c.key == bindparam('key'),
                work_items.c.id == work_item_state_transitions.c.work_item_id
            )
        )