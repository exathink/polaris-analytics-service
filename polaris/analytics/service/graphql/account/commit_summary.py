# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
import datetime
from sqlalchemy import text
from polaris.common import db

from ..interfaces import NamedNode, CommitSummary, KeyIdResolverMixin
from .enums import AccountPartitions

class AccountCommitSummary(graphene.ObjectType, KeyIdResolverMixin):

    class Meta:
        interfaces = (NamedNode, CommitSummary)

    @classmethod
    def resolve(cls, account, info, **kwargs):
        query = None
        if kwargs.get('group_by') == AccountPartitions.account:
            query = commit_summary(account.id)
        elif kwargs.get('group_by') == AccountPartitions.organization:
            query = commit_summary_by_organization(account)
        elif kwargs.get('group_by') == AccountPartitions.project:
            query =  commit_summary_by_project(account)
        elif kwargs.get('group_by') == AccountPartitions.repository:
            query = commit_summary_by_repository(account)

        with db.create_session() as session:
            return session.connection.execute(text(query), dict(account_key=account.id, type=kwargs.get('group_by').value)).fetchall()


def commit_summary(account):
    return """
                SELECT
                  repo_summary.*,
                  contributor_aliases + contributor_keys AS contributor_count
                FROM
                  (
                    SELECT
                      :type                                       AS atype,
                      min(account_key::text)                      AS key,
                      min(accounts.name)                          AS name, 
                      min(earliest_commit)             AS earliest_commit,
                      max(latest_commit)               AS latest_commit,
                      sum(commit_count)                AS commit_count
                    FROM
                      repos.repositories
                      INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                      INNER JOIN repos.accounts_organizations ON organizations.id = accounts_organizations.organization_id
                      INNER JOIN repos.accounts ON accounts_organizations.account_id = accounts.id
                    WHERE account_key = :account_key
                  )
                    AS repo_summary
                  CROSS JOIN
                  (
                    SELECT count(DISTINCT contributor_aliases.id) AS contributor_aliases
                    FROM
                      repos.contributor_aliases
                      INNER JOIN repos.repositories_contributor_aliases
                        ON contributor_aliases.id = repositories_contributor_aliases.contributor_alias_id
                      INNER JOIN repos.repositories ON repositories_contributor_aliases.repository_id = repositories.id
                      INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                      INNER JOIN repos.accounts_organizations ON organizations.id = accounts_organizations.organization_id
                      INNER JOIN repos.accounts ON accounts_organizations.account_id = accounts.id
                    WHERE account_key = :account_key AND contributor_key IS NULL AND not robot

                  )
                    AS contributors_aliases
                  CROSS JOIN
                  (
                    SELECT count(DISTINCT contributor_key) AS contributor_keys
                    FROM
                      repos.contributor_aliases
                      INNER JOIN repos.repositories_contributor_aliases
                        ON contributor_aliases.id = repositories_contributor_aliases.contributor_alias_id
                      INNER JOIN repos.repositories ON repositories_contributor_aliases.repository_id = repositories.id
                      INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                      INNER JOIN repos.accounts_organizations ON organizations.id = accounts_organizations.organization_id
                      INNER JOIN repos.accounts ON accounts_organizations.account_id = accounts.id
                    WHERE account_key = :account_key AND contributor_key IS NOT NULL AND not robot
                  ) AS contributor_keys
            """

def commit_summary_by_organization(account):
    return """
                SELECT
                      :type                                    AS type,
                      org_repo_summary.organization_key::text  AS key,
                      org_repo_summary.organization     AS name,
                      earliest_commit,
                      latest_commit,
                      commit_count,
                      contributor_count
                FROM
                (
                   SELECT
                     organizations.id                            AS organization_id,
                     min(organizations.organization_key::text)   AS organization_key,
                     min(organizations.name)                     AS organization,
                     min(earliest_commit)                        AS earliest_commit,
                     max(latest_commit)                          AS latest_commit,
                     sum(commit_count)                           AS commit_count
                   FROM repos.repositories
                     INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                     INNER JOIN repos.accounts_organizations ON organizations.id = accounts_organizations.organization_id
                     INNER JOIN repos.accounts ON accounts_organizations.account_id = accounts.id
                   WHERE account_key = :account_key
                   GROUP BY organizations.id
                ) AS org_repo_summary
                INNER JOIN
                (
                   SELECT
                     organization_id,
                     sum(contributor_count) AS contributor_count
                   FROM
                     (
                       SELECT
                         repositories.organization_id,
                         count(DISTINCT contributor_alias_id) AS contributor_count
                       FROM
                         repos.repositories
                         INNER JOIN repos.repositories_contributor_aliases
                           ON repositories.id = repositories_contributor_aliases.repository_id
                         INNER JOIN repos.contributor_aliases
                           ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                         INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                         INNER JOIN repos.accounts_organizations
                           ON accounts_organizations.organization_id = organizations.id
                         INNER JOIN repos.accounts ON accounts_organizations.account_id = accounts.id
                       WHERE account_key = :account_key AND contributor_aliases.contributor_key IS NULL AND not robot
                       GROUP BY repositories.organization_id
                       UNION
                       SELECT
                         repositories.organization_id,
                         count(DISTINCT contributor_key) AS contributor_count
                       FROM
                         repos.repositories
                         INNER JOIN repos.repositories_contributor_aliases
                           ON repositories.id = repositories_contributor_aliases.repository_id
                         INNER JOIN repos.contributor_aliases
                           ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                         INNER JOIN repos.organizations ON repositories.organization_id = organizations.id
                         INNER JOIN repos.accounts_organizations
                           ON accounts_organizations.organization_id = organizations.id
                         INNER JOIN repos.accounts ON accounts_organizations.account_id = accounts.id
                       WHERE account_key = :account_key AND contributor_aliases.contributor_key IS NOT NULL AND not robot
                       GROUP BY repositories.organization_id
                     ) _
                   GROUP BY organization_id
                ) AS org_contributor_summary
                ON org_repo_summary.organization_id = org_contributor_summary.organization_id
            """


def commit_summary_by_project(account):
    return """
                SELECT
                          :type                                             AS type,
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

def commit_summary_by_repository(account):
    return """
          SELECT
            :type                                       AS type,
            repositories.key::text                      AS key,
            repositories.name                           AS name,
            repositories.earliest_commit                AS earliest_commit,
            repositories.latest_commit                  AS latest_commit,
            repositories.commit_count                   AS commit_count,
            repo_contributor_summary.contributor_count  AS contributor_count
          FROM
            repos.repositories
            INNER JOIN repos.organizations on repositories.organization_id = organizations.id
            INNER JOIN repos.accounts_organizations on organizations.id = accounts_organizations.organization_id
            INNER JOIN repos.accounts on accounts_organizations.account_id = accounts.id
            INNER JOIN (
                SELECT
                 repository_id,
                 sum(contributor_count) AS contributor_count
                FROM
                  (
                    SELECT
                      repositories.id as repository_id,
                      count(DISTINCT contributor_alias_id) AS contributor_count
                    FROM
                      repos.repositories
                      INNER JOIN repos.repositories_contributor_aliases
                        ON repositories.id = repositories_contributor_aliases.repository_id
                      INNER JOIN repos.contributor_aliases
                        ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                      INNER JOIN repos.organizations on repositories.organization_id = organizations.id
                      INNER JOIN repos.accounts_organizations on organizations.id = accounts_organizations.organization_id
                      INNER JOIN repos.accounts on accounts_organizations.account_id = accounts.id
                    WHERE accounts.account_key = :account_key AND contributor_aliases.contributor_key IS NULL AND not robot
                    GROUP BY repositories.id
                    UNION
                    SELECT
                      repositories.id as repository_id,
                      count(DISTINCT contributor_alias_id) AS contributor_count
                    FROM
                      repos.repositories
                      INNER JOIN repos.repositories_contributor_aliases
                        ON repositories.id = repositories_contributor_aliases.repository_id
                      INNER JOIN repos.contributor_aliases
                        ON repositories_contributor_aliases.contributor_alias_id = contributor_aliases.id
                      INNER JOIN repos.organizations on repositories.organization_id = organizations.id
                      INNER JOIN repos.accounts_organizations on organizations.id = accounts_organizations.organization_id
                      INNER JOIN repos.accounts on accounts_organizations.account_id = accounts.id
                    WHERE accounts.account_key = :account_key  AND contributor_aliases.contributor_key IS NOT NULL AND not robot
                    GROUP BY repositories.id
                  ) _
                  GROUP BY repository_id
              ) repo_contributor_summary
            ON repositories.id = repo_contributor_summary.repository_id
            WHERE accounts.account_key = :account_key
          """


