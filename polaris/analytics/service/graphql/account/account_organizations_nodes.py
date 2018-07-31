# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from sqlalchemy import text, select, bindparam
from polaris.common import db

from polaris.repos.db.model import accounts, accounts_organizations, organizations

from ..interfaces import NamedNode
from ..utils import SQlQueryMeasureResolver


class AccountOrganizationsNodes(SQlQueryMeasureResolver):
    interface = NamedNode
    query = """
                SELECT 
                    organizations.id as id, 
                    organizations.organization_key as key, 
                    organizations.name as name
                FROM 
                    repos.organizations
                    INNER JOIN repos.accounts_organizations ON accounts_organizations.organization_id = organizations.id
                    INNER JOIN repos.accounts on accounts_organizations.account_id = accounts.id
                WHERE accounts.account_key = :account_key      
            
            """

    @classmethod
    def selectable(cls):
        return  select([
            organizations.c.id,
            organizations.c.organization_key.label('key'),
            organizations.c.name
        ]).select_from(
            organizations.join(accounts_organizations).join(accounts)
        ).where(accounts.c.account_key == bindparam('account_key'))


    @classmethod
    def resolve(cls, account_key, info, **kwargs):
        with db.create_session() as session:
            return session.connection.execute(text(cls.query), dict(account_key=account_key)).fetchall()




