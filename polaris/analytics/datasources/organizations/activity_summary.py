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


class ActivitySummaryByProject(Schema):
    organization = fields.String(required=True)
    project_id = fields.Integer(required=True)
    project = fields.String(required=True)
    project_group = fields.String(required=False)
    tags = fields.List(fields.String())
    description = fields.String(required=False)
    website = fields.String(required=False)
    issues_url = fields.String(required=False)
    earliest_commit = fields.DateTime(required=True, format=datetime_utils.DATETIME_NO_TZ)
    latest_commit = fields.DateTime(required=True, format=datetime_utils.DATETIME_NO_TZ)
    commit_count = fields.Integer(required=True)
    contributor_count = fields.Integer(required=True)
    repo_count = fields.Integer(required=True)

    def for_organization(self, organization_name):
        query = text(
            """
              SELECT
                  projects.organization as organization,
                  coalesce(projects.id, -1) as project_id, 
                  coalesce(projects.name, 'Default') AS project,
                  projects.properties->>'project_group' AS project_group,
                  projects.properties->'tags'           AS tags,
                  projects.properties->>'description'   AS description,
                  projects.properties->>'website'       AS website,
                  projects.properties->>'issues_url'    AS issues_url,
                  project_summaries.earliest_commit     AS earliest_commit,
                  project_summaries.latest_commit       AS latest_commit,
                  project_summaries.commit_count        AS commit_count, 
                  project_summaries.contributor_count   AS contributor_count,
                  project_summaries.repo_count          AS repo_count
            FROM
              (
                SELECT
                  prs.*, 
                  pcs.contributor_count
                FROM
                  (
                    SELECT
                      coalesce(project_id, -1) AS project_id,
                      min(projects.name)       AS project_name,
                      count(repositories.id)   AS repo_count,
                      sum(commit_count)        AS commit_count,
                      min(earliest_commit)     AS earliest_commit,
                      max(latest_commit)       AS latest_commit
                    FROM
                      repos.organizations
                      INNER JOIN repos.repositories ON organizations.id = repositories.organization_id
                      LEFT OUTER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                      LEFT OUTER JOIN repos.projects ON projects_repositories.project_id = projects.id
                    WHERE organizations.name = :organization_name
                    GROUP BY project_id
                  ) AS prs
                  INNER JOIN
                  (
                    SELECT
                      coalesce(project_id, -1) AS project_id,
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
                          LEFT OUTER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          LEFT OUTER JOIN repos.projects ON projects_repositories.project_id = projects.id
                        WHERE organizations.name = :organization_name AND contributor_aliases.contributor_key IS NULL
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
                          LEFT OUTER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                          LEFT OUTER JOIN repos.projects ON projects_repositories.project_id = projects.id
                        WHERE organizations.name = :organization_name AND contributor_aliases.contributor_key IS NOT NULL
                        GROUP BY project_id
                      ) AS _
                    GROUP BY project_id
                  ) AS pcs ON prs.project_id = pcs.project_id
              ) AS project_summaries
            LEFT OUTER JOIN (
              SELECT 
                organizations.name as organization, 
                projects.*
              FROM 
                repos.projects
                INNER JOIN repos.organizations ON projects.organization_id = organizations.id
              WHERE organizations.name=:organization_name
            ) AS projects
            ON project_summaries.project_id=projects.id
            """
        )
        with db.create_session() as session:
            results = session.connection.execute(query, dict(organization_name=organization_name)).fetchall()
            return self.dumps(results, many=True)


class ActivitySummary(Schema):
    organization = fields.String(required=True)
    organization_id = fields.Integer(required=True)
    organization_key = fields.String(required=True)
    commit_count = fields.Integer(required=True)
    earliest_commit = fields.DateTime(required=True, format=datetime_utils.DATETIME_NO_TZ)
    latest_commit = fields.DateTime(required=True, format=datetime_utils.DATETIME_NO_TZ)
    contributor_count = fields.Integer(required=True)

    def for_organization(self, organization_name):
        query = text(
            """
            SELECT
              organization_id,
              organization_key,
              organization,
              commit_count,
              earliest_commit,
              latest_commit,
              contributor_count
            FROM
            (
              /* Aggregate commit stats for all repos in this organization*/
              SELECT
              organizations.id AS organization_id,
              min(organizations.name) AS organization, 
              min(organizations.organization_key::TEXT) AS organization_key,
              sum(commit_count) AS commit_count,
              MIN (earliest_commit) AS earliest_commit,
              max(latest_commit) AS latest_commit
              FROM
              repos.organizations
              INNER JOIN repos.repositories ON organizations.id = repositories.organization_id
              WHERE organizations.name = :organization_name
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
                    WHERE organizations.name = :organization_name AND contributor_key IS NULL
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
                    WHERE organizations.name = :organization_name AND contributor_key IS NOT NULL
                    GROUP BY organizations.id
                  ) 
                  AS _
                ) 
                AS organization_contributor_stats  
            ON organization_repo_stats.organization_id = organization_contributor_stats.org_id
            """
        )
        with db.create_session() as session:
            results = session.connection.execute(query, dict(organization_name=organization_name)).fetchall()
            return self.dumps(results, many=True)
