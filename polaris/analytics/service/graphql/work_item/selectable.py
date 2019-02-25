# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import select, bindparam
from polaris.analytics.service.graphql.interfaces import WorkItemInfo
from polaris.analytics.db.model import work_items
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
