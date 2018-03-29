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


class ProjectActivitySummary(Schema):
    organization = fields.String(required=True)
    id = fields.Integer(required=True)
    project = fields.String(required=True)
    project_group = fields.String(required=False)
    tags = fields.List(fields.String())
    description = fields.String(required=False)
    website = fields.String(required=False)
    issues_url = fields.String(required=False)
    repository = fields.String(required=True)
    earliest_commit = fields.DateTime(required=True, format=datetime_utils.DATETIME_NO_TZ)
    latest_commit = fields.DateTime(required=True, format=datetime_utils.DATETIME_NO_TZ)
    commit_count = fields.Integer(required=True)
    contributor_count = fields.Integer(required=True)

    def for_organization(self, organization_name):
        query = text(
            """
              SELECT
                op_summary.organization,
                projects.id                           AS project_id,
                projects.name :: TEXT                 AS project,
                projects.properties->>'project_group' AS project_group,
                projects.properties->'tags'           AS tags,
                projects.properties->>'description'   AS description,
                projects.properties->>'website'       AS website,
                projects.properties->>'issues_url'    AS issues_url,
                op_summary.repository :: TEXT         AS repository,
                op_summary.earliest_commit            AS earliest_commit,
                op_summary.latest_commit              AS latest_commit,
                op_summary.commit_count::BIGINT       AS commit_count,
                op_summary.contributor_count
              FROM
                (
                  SELECT
                    projects.id                             AS project_id,
                    min(organizations.name)                      AS organization,
                    min(repositories.name)                  AS repository,
                    min(repositories.earliest_commit)       AS earliest_commit,
                    max(repositories.latest_commit)         AS latest_commit,
                    sum(repositories.commit_count)          AS commit_count,
                    count(DISTINCT contributor_alias_id)    AS contributor_count
                  FROM
                    repos.organizations
                    INNER JOIN repos.projects ON organizations.id = projects.organization_id
                    INNER JOIN repos.projects_repositories ON projects.id = projects_repositories.project_id
                    INNER JOIN repos.repositories ON projects_repositories.repository_id = repositories.id
                    INNER JOIN repos.repositories_contributor_aliases ON repositories.id = repositories_contributor_aliases.repository_id
                  WHERE
                  organizations.name = :organization_name
                  GROUP BY organizations.id, projects.id
              ) AS op_summary
              INNER JOIN
                repos.projects ON op_summary.project_id = projects.id;
          """
        )
        with db.create_session() as session:
            results = session.connection.execute(query, dict(organization_name=organization_name)).fetchall()
            return self.dumps(results, many=True)
