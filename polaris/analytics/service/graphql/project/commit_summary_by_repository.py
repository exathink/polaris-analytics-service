# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from sqlalchemy import text
from polaris.common import db

from ..interfaces import NamedNode, KeyIdResolverMixin, CommitSummary


class ProjectCommitSummaryByRepository(graphene.ObjectType, KeyIdResolverMixin):
    class Meta:
        interfaces = (NamedNode, CommitSummary)

    @classmethod
    def resolve(cls, project, info, **kwargs):
        query = """
                SELECT
                  :type                    as type, 
                  repo_stats.repository_key as key,
                  repository_name          as name,
                  earliest_commit,
                  latest_commit,
                  commit_count,
                  contributor_count
                FROM
                  (
                    SELECT
                      repositories.id    AS repository_id,
                      repositories.key   AS repository_key, 
                      repositories.name  AS repository_name,
                      earliest_commit,
                      latest_commit,
                      commit_count
                    FROM
                      repos.repositories
                      INNER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                      INNER JOIN repos.projects ON projects_repositories.project_id = projects.id
                    WHERE projects.project_key = :project_key
                  )
                    AS repo_stats
                  INNER JOIN
                  (
                    SELECT
                      repository_id,
                      sum(contributor_count) AS contributor_count
                    FROM
                      (
                        SELECT
                          repositories.id                      AS repository_id,
                          count(DISTINCT contributor_alias_id) AS contributor_count
                        FROM
                          repos.repositories
                          INNER JOIN repos.repositories_contributor_aliases
                            ON repositories.id = repositories_contributor_aliases.repository_id
                          INNER JOIN repos.contributor_aliases
                            ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                          INNER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          INNER JOIN repos.projects ON projects_repositories.project_id = projects.id
                        WHERE  projects.project_key = :project_key AND contributor_key IS NULL AND
                              NOT robot
                        GROUP BY repositories.id
                        UNION
                        SELECT
                          repositories.id                 AS repository_id,
                          count(DISTINCT contributor_key) AS contributor_count
                        FROM
                          repos.repositories
                          INNER JOIN repos.repositories_contributor_aliases
                            ON repositories.id = repositories_contributor_aliases.repository_id
                          INNER JOIN repos.contributor_aliases
                            ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                          INNER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          INNER JOIN repos.projects ON projects_repositories.project_id = projects.id
                        WHERE
                          projects.project_key = :project_key AND contributor_key IS NOT NULL AND
                          NOT robot
                        GROUP BY repositories.id
                      ) AS _
                    GROUP BY repository_id
                  ) AS repo_contributor_stats
                    ON repo_stats.repository_id = repo_contributor_stats.repository_id
            """
        with db.create_session() as session:
            return session.connection.execute(text(query), dict(project_key=project.id,
                                                                type=kwargs.get('group_by'))).fetchall()












