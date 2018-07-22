# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import text
from polaris.common import db


class AccountProjectsCommitSummaries:
    @classmethod
    def resolve(cls, account_key, info, **kwargs):
        query = """
                SELECT
                          prj_repo_summary.project_key                      AS key,
                          prj_repo_summary.project                          AS name,
                          earliest_commit,
                          latest_commit,
                          commit_count,
                          contributor_count
                    FROM
                    (
                       SELECT
                         projects.id                                 AS project_id,
                         min(projects.project_key::text)             AS project_key,
                         min(projects.name)                          AS project,
                         min(earliest_commit)                        AS earliest_commit,
                         max(latest_commit)                          AS latest_commit,
                         sum(commit_count)                           AS commit_count
                       FROM repos.repositories
                         INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                         INNER JOIN repos.accounts_organizations ON organizations.id = accounts_organizations.organization_id
                         INNER JOIN repos.accounts ON accounts_organizations.account_id = accounts.id
                         INNER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
                         INNER JOIN repos.projects ON projects_repositories.project_id = projects.id
                       WHERE account_key = :account_key
                       GROUP BY projects.id
                    ) AS prj_repo_summary
                    INNER JOIN
                    (
                       SELECT
                         project_id,
                         sum(contributor_count) AS contributor_count
                       FROM
                         (
                           SELECT
                             projects.id as project_id,
                             count(DISTINCT contributor_alias_id) AS contributor_count
                           FROM
                             repos.repositories
                             INNER JOIN repos.repositories_contributor_aliases
                               ON repositories.id = repositories_contributor_aliases.repository_id
                             INNER JOIN repos.contributor_aliases
                               ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                             INNER JOIN repos.projects_repositories
                               ON repositories.id = projects_repositories.repository_id
                             INNER JOIN repos.projects 
                               ON projects_repositories.project_id = projects.id
                             INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                             INNER JOIN repos.accounts_organizations ON organizations.id = accounts_organizations.organization_id
                             INNER JOIN repos.accounts ON accounts_organizations.account_id = accounts.id
    
                           WHERE account_key = :account_key AND contributor_aliases.contributor_key IS NULL AND not robot
                           GROUP BY projects.id
                           UNION
                           SELECT
                             projects.id as project_id,
                             count(DISTINCT contributor_alias_id) AS contributor_count
                           FROM
                             repos.repositories
                             INNER JOIN repos.repositories_contributor_aliases
                               ON repositories.id = repositories_contributor_aliases.repository_id
                             INNER JOIN repos.contributor_aliases
                               ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                             INNER JOIN repos.projects_repositories
                               ON repositories.id = projects_repositories.repository_id
                             INNER JOIN repos.projects 
                               ON projects_repositories.project_id = projects.id
                             INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                             INNER JOIN repos.accounts_organizations ON organizations.id = accounts_organizations.organization_id
                             INNER JOIN repos.accounts ON accounts_organizations.account_id = accounts.id
    
                           WHERE account_key = :account_key AND contributor_aliases.contributor_key IS NOT NULL AND not robot
                           GROUP BY projects.id
                         ) _
                       GROUP BY project_id
                    ) AS prj_contributor_summary
                    ON prj_repo_summary.project_id = prj_contributor_summary.project_id
            """
        with db.create_session() as session:
            return session.connection.execute(text(query), dict(account_key=account_key)).fetchall()








