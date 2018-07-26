# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.common import db
from sqlalchemy import text

class AccountOrganizationsCount:

    @classmethod
    def resolve(cls, key, info, **kwargs):
        query = """
            SELECT count(organization_id) 
            FROM repos.accounts_organizations 
            INNER JOIN repos.accounts on accounts_organizations.account_id = accounts.id
            WHERE accounts.account_key = :key
        """
        with db.create_session() as session:
            return session.execute(text(query), dict(key=key)).scalar()
