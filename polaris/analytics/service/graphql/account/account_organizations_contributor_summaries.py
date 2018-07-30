# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import text
from polaris.common import db
from ..interfaces import ContributorSummary
from ..utils import SQlQueryMeasureResolver


class AccountOrganizationsContributorSummaries(SQlQueryMeasureResolver):
    interface = ContributorSummary

    query = """
        WITH account_orgs AS (
          SELECT organizations.id as id
          FROM
               repos.organizations
                 INNER JOIN repos.accounts_organizations on accounts_organizations.organization_id=organizations.id
                 INNER JOIN repos.accounts on accounts_organizations.account_id = accounts.id
          WHERE accounts.account_key = :account_key
        ), contributor_summary AS (
          SELECT account_orgs.id, contributor_aliases.id as ca_id, contributor_aliases.contributor_key
          FROM
               account_orgs
                 LEFT JOIN repos.repositories on account_orgs.id = repositories.organization_id
                 LEFT JOIN repos.repositories_contributor_aliases on repositories.id = repositories_contributor_aliases.repository_id
                 LEFT JOIN repos.contributor_aliases on repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                 WHERE NOT contributor_aliases.robot
        
        ), unique_contributors AS (
            SELECT id, count(distinct contributor_key) as unique_contributor_count
            FROM contributor_summary
            GROUP BY id
        ), unassigned_aliases AS (
            SELECT id, count(distinct ca_id) AS unassigned_alias_count
            FROM contributor_summary WHERE contributor_key IS NULL
            GROUP BY id
        ) 
        SELECT 
                 unique_contributors.id, 
                 (CASE WHEN unassigned_alias_count IS NULL THEN 0 ELSE unassigned_alias_count END) AS unassigned_alias_count,
                 unique_contributor_count,  
                 (CASE WHEN unassigned_alias_count IS NULL THEN 0 ELSE unassigned_alias_count END) + unique_contributor_count AS contributor_count
        FROM
             unique_contributors 
               LEFT JOIN unassigned_aliases ON unique_contributors.id=unassigned_aliases.id
    """
    @classmethod
    def resolve(cls, account_key, info, **kwargs):
        with db.create_session() as session:
            return session.connection.execute(text(cls.query), dict(account_key=account_key)).fetchall()
