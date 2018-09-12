# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from ..interfaces import NamedNode
from sqlalchemy import select, func, bindparam
from polaris.repos.db.schema import commits


class CommitNode:
    interface = NamedNode

    @staticmethod
    def selectable(**kwargs):
        return select([
            commits.c.id,
            commits.c.key,
            func.substr(commits.c.key, 1, 12).label('name')

        ]).select_from(
            commits
        ).where(
            commits.c.key == bindparam('key')
        )
