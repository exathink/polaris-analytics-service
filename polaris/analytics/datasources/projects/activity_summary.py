# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from marshmallow import Schema, fields
from sqlalchemy import text
from polaris.common import db
from polaris.utils import datetime_utils


class ActivitySummaryByRepository(Schema):
    project = fields.String(required=True)
    repository_id = fields.Integer(required=True)
    repository = fields.String(required=True)
    repository_group = fields.String(required=False)
    tags = fields.List(fields.String())
    description = fields.String(required=False)
    website = fields.String(required=False)
    issues_url = fields.String(required=False)
    earliest_commit = fields.DateTime(required=True, format=datetime_utils.DATETIME_NO_TZ)
    latest_commit = fields.DateTime(required=True, format=datetime_utils.DATETIME_NO_TZ)
    commit_count = fields.Integer(required=True)
    contributor_count = fields.Integer(required=True)


    def for_project(self, organization_name, project_name):
        if project_name == 'Default':
            return self.for_default_project(organization_name)
        else:
            query = text(
                """
                    SELECT
                      repo_stats.repository_id,
                      organization,
                      project,
                      repository,
                      repository_group,
                      tags,
                      description,
                      website,
                      issues_url,
                      earliest_commit,
                      latest_commit,
                      commit_count,
                      contributor_count
                    FROM
                      (
                        SELECT
                          repositories.id    AS repository_id,
                          organizations.name AS organization,
                          projects.name      AS project,
                          repositories.name  AS repository,
                          repositories.properties->>'project_group' AS repository_group,
                          repositories.properties->'tags'           AS tags,
                          repositories.properties->>'description'   AS description,
                          repositories.properties->>'website'       AS website,
                          repositories.properties->>'issues_url'    AS issues_url,
                          earliest_commit,
                          latest_commit,
                          commit_count
                        FROM
                          repos.repositories
                          INNER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          INNER JOIN repos.projects ON projects_repositories.project_id = projects.id
                          INNER JOIN repos.organizations ON projects.organization_id = organizations.id
                        WHERE organizations.name = :organization_name AND projects.name = :project_name
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
                              INNER JOIN repos.organizations ON projects.organization_id = organizations.id
                            WHERE organizations.name = :organization_name AND projects.name = :project_name AND contributor_key IS NULL AND
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
                              INNER JOIN repos.organizations ON projects.organization_id = organizations.id
                            WHERE
                              organizations.name = :organization_name AND projects.name = :project_name AND contributor_key IS NOT NULL AND
                              NOT robot
                            GROUP BY repositories.id
                          ) AS _
                        GROUP BY repository_id
                      ) AS repo_contributor_stats
                        ON repo_stats.repository_id = repo_contributor_stats.repository_id
                """
            )
            with db.create_session() as session:
                results = session.connection.execute(query, dict(organization_name=organization_name, project_name=project_name)).fetchall()
                return self.dumps(results, many=True)

    def for_default_project(self, organization_name):
        query = text(
            """
                SELECT
                  repo_stats.repository_id,
                  organization,
                  project,
                  repository,
                  repository_group,
                  tags,
                  description,
                  website,
                  issues_url,
                  earliest_commit,
                  latest_commit,
                  commit_count,
                  contributor_count
                FROM
                  (
                    SELECT
                      repositories.id    AS repository_id,
                      organizations.name AS organization,
                      'Default'      AS project,
                      repositories.name  AS repository,
                      repositories.properties->>'project_group' AS repository_group,
                      repositories.properties->'tags'           AS tags,
                      repositories.properties->>'description'   AS description,
                      repositories.properties->>'website'       AS website,
                      repositories.properties->>'issues_url'    AS issues_url,
                      earliest_commit,
                      latest_commit,
                      commit_count
                    FROM
                      repos.repositories
                      LEFT OUTER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                      INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                    WHERE organizations.name = :organization_name AND project_id is NULL
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
                          LEFT OUTER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                        WHERE organizations.name = :organization_name AND project_id is NULL AND contributor_key IS NULL AND
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
                          LEFT OUTER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                        WHERE
                          organizations.name = :organization_name AND project_id is NULL AND contributor_key IS NOT NULL AND
                          NOT robot
                        GROUP BY repositories.id
                      ) AS _
                    GROUP BY repository_id
                  ) AS repo_contributor_stats
                    ON repo_stats.repository_id = repo_contributor_stats.repository_id
            """
        )
        with db.create_session() as session:
            results = session.connection.execute(query, dict(organization_name=organization_name)).fetchall()
            return self.dumps(results, many=True)

class ActivitySummary(Schema):
    project = fields.String(required=True)
    organization_id = fields.Integer(required=True)
    organization_key = fields.String(required=True)
    commit_count = fields.Integer(required=True)
    earliest_commit = fields.DateTime(required=True, format=datetime_utils.DATETIME_NO_TZ)
    latest_commit = fields.DateTime(required=True, format=datetime_utils.DATETIME_NO_TZ)
    contributor_count = fields.Integer(required=True)

    def for_project(self, organization_name, project_name):
        if project_name == 'Default':
            return self.for_default_project(organization_name)
        else:
            query = text(
                """
                    SELECT
                      organization,
                      project,
                      earliest_commit,
                      latest_commit,
                      commit_count,
                      contributor_count
                    FROM
                      (
                        SELECT
                          projects.id             AS project_id,
                          min(organizations.name) AS organization,
                          min(projects.name)      AS project,
                          min(earliest_commit)    AS earliest_commit,
                          max(latest_commit)      AS latest_commit,
                          sum(commit_count)       AS commit_count
                        FROM
                          repos.repositories
                          INNER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          INNER JOIN repos.projects ON projects_repositories.project_id = projects.id
                          INNER JOIN repos.organizations ON projects.organization_id = organizations.id
                        WHERE organizations.name = :organization_name AND projects.name = :project_name
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
                              INNER JOIN repos.organizations ON projects.organization_id = organizations.id
                            WHERE organizations.name = :organization_name AND projects.name = :project_name AND contributor_key IS NULL AND
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
                              INNER JOIN repos.organizations ON projects.organization_id = organizations.id
                            WHERE
                              organizations.name = :organization_name AND projects.name = :project_name AND contributor_key IS NOT NULL AND
                              NOT robot
                            GROUP BY projects.id
                          ) AS _
                        GROUP BY project_id
                      ) AS repo_contributor_stats
                        ON repo_stats.project_id = repo_contributor_stats.project_id
                """
            )
            with db.create_session() as session:
                results = session.connection.execute(query, dict(organization_name=organization_name, project_name=project_name)).fetchall()
                return self.dumps(results, many=True)

    def for_default_project(self, organization_name):
        query = text(
            """
                SELECT
                  organization,
                  project,
                  earliest_commit,
                  latest_commit,
                  commit_count,
                  contributor_count
                FROM
                  (
                    SELECT
                      min(organizations.name) AS organization,
                      'Default'               AS project,
                      min(earliest_commit)    AS earliest_commit,
                      max(latest_commit)      AS latest_commit,
                      sum(commit_count)       AS commit_count
                    FROM
                      repos.repositories
                      LEFT OUTER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                      INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                    WHERE organizations.name = :organization_name AND project_id IS NULL
                  )
                    AS repo_stats
                  CROSS JOIN
                  (
                    SELECT sum(contributor_count) AS contributor_count
                    FROM
                      (
                        SELECT count(DISTINCT contributor_alias_id) AS contributor_count
                        FROM
                          repos.repositories
                          INNER JOIN repos.repositories_contributor_aliases
                            ON repositories.id = repositories_contributor_aliases.repository_id
                          INNER JOIN repos.contributor_aliases
                            ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                          LEFT OUTER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                        WHERE organizations.name = :organization_name AND project_id IS NULL AND contributor_key IS NULL AND
                              NOT robot
                        UNION
                        SELECT count(DISTINCT contributor_key) AS contributor_count
                        FROM
                          repos.repositories
                          INNER JOIN repos.repositories_contributor_aliases
                            ON repositories.id = repositories_contributor_aliases.repository_id
                          INNER JOIN repos.contributor_aliases
                            ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                          LEFT OUTER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                        WHERE
                          organizations.name = :organization_name AND project_id IS NULL AND contributor_key IS NOT NULL AND
                          NOT robot
                      ) AS _
                  ) AS repo_contributor_stats

            """
        )
        with db.create_session() as session:
            results = session.connection.execute(query, dict(organization_name=organization_name)).fetchall()
            return self.dumps(results, many=True)
