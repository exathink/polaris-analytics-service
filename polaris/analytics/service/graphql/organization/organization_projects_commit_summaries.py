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


class OrganizationProjectsCommitSummaries(graphene.ObjectType, KeyIdResolverMixin):
    class Meta:
        interfaces = (NamedNode, CommitSummary)

    @classmethod
    def resolve(cls, organization_key, info, **kwargs):
        query = """
                SELECT
                  prs.project_key as key, 
                  prs.project_name as name, 
                  prs.commit_count,
                  prs.earliest_commit, 
                  prs.latest_commit,
                  pcs.contributor_count
                FROM
                  (
                    SELECT
                      project_id,
                      min(projects.project_key::text) as project_key,
                      min(projects.name)       AS project_name,
                      sum(commit_count)        AS commit_count,
                      min(earliest_commit)     AS earliest_commit,
                      max(latest_commit)       AS latest_commit
                    FROM
                      repos.organizations
                      INNER JOIN repos.repositories ON organizations.id = repositories.organization_id
                      INNER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                      INNER JOIN repos.projects ON projects_repositories.project_id = projects.id
                    WHERE organizations.organization_key = :organization_key
                    GROUP BY project_id
                  ) AS prs
                  INNER JOIN
                  (
                    SELECT
                      project_id,
                      sum(contributor_count)   AS contributor_count
                    FROM
                      (
                        SELECT
                          project_id,
                          COUNT(DISTINCT contributor_alias_id) AS contributor_count
                        FROM
                          repos.organizations
                          INNER JOIN repos.repositories ON organizations.id = repositories.organization_id
                          INNER JOIN repos.repositories_contributor_aliases
                            ON repositories.id = repositories_contributor_aliases.repository_id
                          INNER JOIN repos.contributor_aliases
                            ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                          INNER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          INNER JOIN repos.projects ON projects_repositories.project_id = projects.id
                        WHERE organizations.organization_key = :organization_key AND contributor_aliases.contributor_key IS NULL AND not robot
                        GROUP BY project_id
                        UNION
                        SELECT
                          project_id,
                          COUNT(DISTINCT contributor_key) AS contributor_count
                        FROM
                          repos.organizations
                          INNER JOIN repos.repositories ON organizations.id = repositories.organization_id
                          INNER JOIN repos.repositories_contributor_aliases
                            ON repositories.id = repositories_contributor_aliases.repository_id
                          INNER JOIN repos.contributor_aliases
                            ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                          INNER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          INNER JOIN repos.projects ON projects_repositories.project_id = projects.id
                        WHERE organizations.organization_key = :organization_key AND contributor_aliases.contributor_key IS NOT NULL AND not robot
                        GROUP BY project_id
                      ) AS _
                    GROUP BY project_id
                  ) AS pcs ON prs.project_id = pcs.project_id
            """
        with db.create_session() as session:
            return session.connection.execute(text(query), dict(organization_key=organization_key)).fetchall()








