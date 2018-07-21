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


class CommitSummaryForProject(graphene.ObjectType, KeyIdResolverMixin):
    class Meta:
        interfaces = (NamedNode, CommitSummary)

    @classmethod
    def resolve(cls, project_key, info, **kwargs):
        query = """
                SELECT
                  project_key                AS key, 
                  project_name               AS name, 
                  earliest_commit,
                  latest_commit,
                  commit_count,
                  contributor_count
                FROM
                  (
                    SELECT
                      projects.id             AS project_id,
                      min(projects.name)      AS project_name,
                      min(projects.project_key::text) as project_key, 
                      min(earliest_commit)    AS earliest_commit,
                      max(latest_commit)      AS latest_commit,
                      sum(commit_count)       AS commit_count
                    FROM
                      repos.repositories
                      INNER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                      INNER JOIN repos.projects ON projects_repositories.project_id = projects.id
                    WHERE projects.project_key = :project_key
                    GROUP BY projects.id
                  )
                    AS repo_stats
                  INNER JOIN
                  (
                    SELECT
                      project_id,
                      sum(contributor_count) AS contributor_count
                    FROM
                      (
                        SELECT
                          projects.id                          AS project_id,
                          count(DISTINCT contributor_alias_id) AS contributor_count
                        FROM
                          repos.repositories
                          INNER JOIN repos.repositories_contributor_aliases
                            ON repositories.id = repositories_contributor_aliases.repository_id
                          INNER JOIN repos.contributor_aliases
                            ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                          INNER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          INNER JOIN repos.projects ON projects_repositories.project_id = projects.id
                        WHERE projects.project_key = :project_key AND contributor_key IS NULL AND
                              NOT robot
                        GROUP BY projects.id
                        UNION
                        SELECT
                          projects.id                     AS project_id,
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
                        GROUP BY projects.id
                      ) AS _
                    GROUP BY project_id
                  ) AS repo_contributor_stats
                    ON repo_stats.project_id = repo_contributor_stats.project_id
            """
        with db.create_session() as session:
            return session.connection.execute(text(query), dict(project_key=project_key)).fetchone()




