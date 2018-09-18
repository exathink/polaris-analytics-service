# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from ..interfaces import CommitInfo
from sqlalchemy import select, func, bindparam, and_
from polaris.repos.db.schema import commits, repositories

from .column_expressions import commit_info_columns


class CommitNode:
    interface = CommitInfo

    @staticmethod
    def selectable(**kwargs):
        return select([
            *commit_info_columns(repositories, commits)
        ]).select_from(
            repositories.join(
                commits
            )
        ).where(
            and_(
                repositories.c.key == bindparam('repository_key'),
                commits.c.key == bindparam('commit_key')
            )
        )
