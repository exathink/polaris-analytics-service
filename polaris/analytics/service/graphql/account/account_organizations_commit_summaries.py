# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import text, select, func
from polaris.common import db
from polaris.repos.db.schema import repositories

from ..interfaces import CommitSummary
from ..utils import SQlQueryMeasureResolver


class AccountOrganizationsCommitSummaries(SQlQueryMeasureResolver):
    interface = CommitSummary
    query = """
               SELECT
                 named_nodes.id                              AS id,
                 min(earliest_commit)                        AS earliest_commit,
                 max(latest_commit)                          AS latest_commit,
                 sum(commit_count)                           AS commit_count
               FROM
               named_nodes  
               LEFT JOIN repos.repositories on named_nodes.id = repositories.organization_id
               GROUP BY named_nodes.id
            """

    @staticmethod
    def selectable(account_organizations_nodes):
        return select([
            account_organizations_nodes.c.id,
            func.sum(repositories.c.commit_count).label('commit_count'),
            func.min(repositories.c.earliest_commit).label('earliest_commit'),
            func.max(repositories.c.latest_commit).label('latest_commit')

        ]).select_from(
            account_organizations_nodes.outerjoin(repositories, account_organizations_nodes.c.id == repositories.c.organization_id)
        ).group_by(account_organizations_nodes.c.id)



    @classmethod
    def resolve(cls, account_key, info, **kwargs):

        with db.create_session() as session:
            return session.connection.execute(text(cls.query), dict(account_key=account_key)).fetchall()




