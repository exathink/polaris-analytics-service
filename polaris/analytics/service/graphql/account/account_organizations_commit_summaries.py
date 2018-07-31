# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import text
from polaris.common import db

from ..interfaces import CommitSummary
from ..utils import SQlQueryMeasureResolver


class AccountOrganizationsCommitSummaries(SQlQueryMeasureResolver):
    interface = CommitSummary
    query = """
               SELECT
                 organizations.id                            AS id,
                 min(earliest_commit)                        AS earliest_commit,
                 max(latest_commit)                          AS latest_commit,
                 sum(commit_count)                           AS commit_count
               FROM repos.repositories
                 INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                 INNER JOIN repos.accounts_organizations ON organizations.id = accounts_organizations.organization_id
                 INNER JOIN repos.accounts ON accounts_organizations.account_id = accounts.id
               WHERE account_key = :account_key
               GROUP BY organizations.id
            """

    @classmethod
    def resolve(cls, account_key, info, **kwargs):

        with db.create_session() as session:
            return session.connection.execute(text(cls.query), dict(account_key=account_key)).fetchall()




