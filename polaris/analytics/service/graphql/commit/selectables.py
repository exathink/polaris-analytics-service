# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from ..interfaces import CommitInfo, FileTypesSummary
from sqlalchemy import select, func, bindparam, and_, case
from polaris.repos.db.schema import commits, repositories, source_file_versions, source_files

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


class CommitFileTypesSummary:
    interface = FileTypesSummary

    @staticmethod
    def selectable(**kwargs):
        return select([
            case(
                [
                    (source_files.c.file_type != None, source_files.c.file_type)
                ],
                else_=''
            ).label('file_type'),
            func.count(source_files.c.id).label('count')

        ]).select_from(
            repositories.join(
                commits, commits.c.repository_id == repositories.c.id
            ).join(
                source_file_versions, source_file_versions.c.commit_id == commits.c.id
            ).join(
                source_files, source_file_versions.c.source_file_id == source_files.c.id
            )
        ).where(
            and_(
                repositories.c.key == bindparam('repository_key'),
                commits.c.key == bindparam('commit_key')
            )
        ).group_by(
            source_files.c.file_type
        )
