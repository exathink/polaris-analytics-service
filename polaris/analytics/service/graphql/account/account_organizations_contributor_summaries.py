# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import text, select, func, distinct,case


from polaris.repos.db.schema import repositories, repositories_contributor_aliases, contributor_aliases

from polaris.common import db
from ..interfaces import ContributorSummary
from ..utils import SQlQueryMeasureResolver


class AccountOrganizationsContributorSummaries(SQlQueryMeasureResolver):
    interface = ContributorSummary

    query = """
        WITH contributor_summary AS (
          SELECT named_nodes.id, contributor_aliases.id as ca_id, contributor_aliases.contributor_key
          FROM
               named_nodes
                 LEFT JOIN repos.repositories on named_nodes.id = repositories.organization_id
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

    @staticmethod
    def selectable(account_organizations_nodes):

        contributor_summmary = select([
            account_organizations_nodes.c.id,
            contributor_aliases.c.id.label('ca_id'),
            contributor_aliases.c.contributor_key
        ]).select_from(
            account_organizations_nodes.outerjoin(
                repositories, repositories.c.organization_id == account_organizations_nodes.c.id
            ).outerjoin(
                repositories_contributor_aliases, repositories.c.id == repositories_contributor_aliases.c.repository_id
            ).outerjoin(
                contributor_aliases, contributor_aliases.c.id == repositories_contributor_aliases.c.contributor_alias_id
            )
        ).where (
            contributor_aliases.c.robot == False
        ).cte()

        unique_contributors = select([
            contributor_summmary.c.id,
            func.count(distinct(contributor_summmary.c.contributor_key)).label('unique_contributor_count'),
        ]).select_from(
            contributor_summmary
        ).group_by(contributor_summmary.c.id).cte()

        unassigned_aliases = select([
            contributor_summmary.c.id,
            func.count(distinct(contributor_summmary.c.ca_id)).label('unassigned_alias_count')
        ]).select_from(
            contributor_summmary
        ).where(contributor_summmary.c.contributor_key == None).\
            group_by(contributor_summmary.c.id).cte()

        return select([
            unique_contributors.c.id,
            case([
                (unassigned_aliases.c.unassigned_alias_count == None, 0),
            ], else_= unassigned_aliases.c.unassigned_alias_count).label('unassigned_alias_count'),
            unique_contributors.c.unique_contributor_count,
            (
                case([
                    (unassigned_aliases.c.unassigned_alias_count == None, 0),
                ], else_ = unassigned_aliases.c.unassigned_alias_count)
                + unique_contributors.c.unique_contributor_count
            ).label('contributor_count')
        ]).select_from(
            unique_contributors.outerjoin(unassigned_aliases, unique_contributors.c.id == unassigned_aliases.c.id)
        )

    @classmethod
    def resolve(cls, account_key, info, **kwargs):
        with db.create_session() as session:
            return session.connection.execute(text(cls.query), dict(account_key=account_key)).fetchall()
