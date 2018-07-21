# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from sqlalchemy import text
from polaris.common import db

from ..interfaces import NamedNode, CommitSummary
from polaris.analytics.service.graphql.mixins import KeyIdResolverMixin


class CommitSummaryForOrganization(graphene.ObjectType, KeyIdResolverMixin):
    class Meta:
        interfaces = (NamedNode, CommitSummary)

    @classmethod
    def resolve(cls, organization_key, info, **kwargs):
        query = """
            SELECT
              key,
              name,
              earliest_commit,
              latest_commit,
              commit_count,
              contributor_count
            FROM
            (
              /* Aggregate commit stats for all repos in this organization*/
              SELECT
              organizations.id AS organization_id,
              min(organizations.name) as name,
              min(organizations.organization_key::text) as key, 
              sum(commit_count) AS commit_count,
              MIN (earliest_commit) AS earliest_commit,
              max(latest_commit) AS latest_commit
              FROM
              repos.organizations
              INNER JOIN repos.repositories ON organizations.id = repositories.organization_id
              WHERE organizations.organization_key = :organization_key
              GROUP BY organizations.id
            ) 
            AS organization_repo_stats
            /* Now join this with the set of distinct contributors for the organization*/
            INNER JOIN
              (
                /* If a contributor_alias has a contributor key then the distinct contributor key is what 
                  counts as the contributor identity. Otherwise we use to the contributor_alias_id to denote
                  unique identity of a contributor. So the total distinct contributors in the org 
                  are the SUM of number of distinct contributor keys for aliases with contributor keys and the number of 
                  distinct contributor_aliases for the aliases without contributor keys
                */
                SELECT
                  min(organization_id)   AS org_id,
                  sum(contributor_count) AS contributor_count
                FROM
                  (
                    /* Number of distinct contributor aliases for aliases without keys*/
                    SELECT
                      organizations.id                     AS organization_id,
                      COUNT(DISTINCT contributor_alias_id) AS contributor_count
                    FROM
                      repos.organizations
                      INNER JOIN repos.repositories ON organizations.id = repositories.organization_id
                      INNER JOIN repos.repositories_contributor_aliases
                        ON repositories.id = repositories_contributor_aliases.repository_id
                      INNER JOIN repos.contributor_aliases
                        ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                    WHERE organizations.organization_key = :organization_key AND contributor_key IS NULL AND not robot
                    GROUP BY organizations.id

                    UNION
                    /* Number of distinct contributor keys for aliases with contributor keys*/
                    SELECT
                      organizations.id                AS organization_id,
                      COUNT(DISTINCT contributor_key) AS contributor_count
                    FROM
                      repos.organizations
                      INNER JOIN repos.repositories ON organizations.id = repositories.organization_id
                      INNER JOIN repos.repositories_contributor_aliases
                        ON repositories.id = repositories_contributor_aliases.repository_id
                      INNER JOIN repos.contributor_aliases
                        ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                    WHERE organizations.organization_key = :organization_key AND contributor_key IS NOT NULL AND not robot
                    GROUP BY organizations.id
                  ) 
                  AS _
                ) 
                AS organization_contributor_stats  
            ON organization_repo_stats.organization_id = organization_contributor_stats.org_id
            """
        with db.create_session() as session:
            return session.connection.execute(text(query), dict(organization_key=organization_key)).fetchone()




