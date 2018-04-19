# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from flask import abort
from sqlalchemy import text
from polaris.common import db

def has_org_access(user_config, organization_name):
    query = text(
        """
        SELECT EXISTS(
            SELECT 
              1  
            FROM 
              repos.accounts
              INNER JOIN repos.accounts_organizations ON accounts.id = accounts_organizations.account_id
              INNER JOIN repos.organizations ON accounts_organizations.organization_id = organizations.id
              WHERE accounts.account_key = :account_key AND organizations.name = :organization_name 
        )
        """
    )
    with db.create_session() as session:
        return session.connection.execute(
            query,
            dict(
                account_key=user_config['account']['account_key'],
                organization_name=organization_name
            )
        ).scalar() or abort(403)

