# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.common import db
from sqlalchemy import text


class ProjectRepositoriesCount:

    @classmethod
    def resolve(cls, key, info, **kwargs):
        query = """
            SELECT count(repositories.id)
            FROM repos.repositories
            INNER JOIN repos.projects_repositories ON repositories.id = projects_repositories.repository_id
            INNER JOIN repos.projects on projects_repositories.project_id = projects.id  
            WHERE projects.project_key = :key
        """
        with db.create_session() as session:
            return session.execute(text(query), dict(key=key)).scalar()
