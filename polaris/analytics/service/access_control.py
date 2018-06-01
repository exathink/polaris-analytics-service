# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from flask import abort
from sqlalchemy import text
from polaris.common import db

def has_org_access(current_user, organization_key):
    if 'admin' in current_user.roles:
        return True

    query = text(
        """
        SELECT EXISTS(
            SELECT 
              1  
            FROM 
              repos.accounts
              INNER JOIN repos.accounts_organizations ON accounts.id = accounts_organizations.account_id
              INNER JOIN repos.organizations ON accounts_organizations.organization_id = organizations.id
              WHERE accounts.account_key = :account_key AND organizations.organization_key = :organization_key 
        )
        """
    )
    with db.create_session() as session:
        return session.connection.execute(
            query,
            dict(
                account_key=current_user.user_config['account']['account_key'],
                organization_key=organization_key
            )
        ).scalar() or abort(403)

def has_project_access(current_user, project_key):
    if 'admin' in current_user.roles:
        return True

    query = text(
        """
        SELECT EXISTS(
            SELECT 
              1  
            FROM 
              repos.accounts
              INNER JOIN repos.accounts_organizations ON accounts.id = accounts_organizations.account_id
              INNER JOIN repos.organizations ON accounts_organizations.organization_id = organizations.id
              INNER JOIN repos.projects on organizations.id = projects.organization_id
              WHERE accounts.account_key = :account_key AND projects.project_key = project_key 
        )
        """
    )
    with db.create_session() as session:
        return session.connection.execute(
            query,
            dict(
                account_key=current_user.user_config['account']['account_key'],
                project_key=project_key
            )
        ).scalar() or abort(403)

