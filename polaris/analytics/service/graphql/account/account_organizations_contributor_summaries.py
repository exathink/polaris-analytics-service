# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import text
from polaris.common import db

class AccountOrganizationsContributorSummaries:
    query = """
                    SELECT
                        min(organizations.organization_key::text) as key, 
                        count(DISTINCT contributor_aliases.id) AS contributor_count
                   FROM
                     repos.repositories
                     INNER JOIN repos.repositories_contributor_aliases
                       ON repositories.id = repositories_contributor_aliases.repository_id
                     INNER JOIN repos.contributor_aliases
                       ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                     INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                     INNER JOIN repos.accounts_organizations
                       ON accounts_organizations.organization_id = organizations.id
                     INNER JOIN repos.accounts ON accounts_organizations.account_id = accounts.id
                   WHERE account_key = :account_key AND not robot
                   GROUP BY repositories.organization_id
                """
    @classmethod
    def resolve(cls, account_key, info, **kwargs):
        with db.create_session() as session:
            return session.connection.execute(text(cls.query), dict(account_key=account_key)).fetchall()




