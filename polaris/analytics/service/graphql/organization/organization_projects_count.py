# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.common import db
from sqlalchemy import text


class OrganizationProjectsCount:

    @classmethod
    def resolve(cls, key, info, **kwargs):
        query = """
            SELECT count(projects.id)
            FROM
            repos.projects
            INNER JOIN repos.organizations ON projects.organization_id = organizations.id  
            WHERE organizations.organization_key = :key
        """
        with db.create_session() as session:
            return session.execute(text(query), dict(key=key)).scalar()
