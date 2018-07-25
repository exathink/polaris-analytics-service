# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.common import db
from sqlalchemy import text

class RepositoryCommitSummary:

    @classmethod
    def resolve(cls, repository_key, info, **kwargs):
        query = """
                SELECT
                  repo_summary.*,
                  contributor_aliases + contributor_keys AS contributor_count
                FROM
                  (
                    SELECT
                      key::text,
                      name, 
                      earliest_commit,
                      latest_commit,
                      commit_count
                    FROM
                      repos.repositories
                    WHERE key = :repository_key
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
                    WHERE repositories.key = :repository_key AND contributor_key IS NULL AND not robot

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
                    WHERE repositories.key = :repository_key AND contributor_key IS NOT NULL AND not robot
                  ) AS contributor_keys
            """
        with db.create_session() as session:
            return session.execute(text(query), dict(repository_key=repository_key)).fetchone()
