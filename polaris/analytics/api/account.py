# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from polaris.auth.db import api as auth_db_api
from polaris.auth.db.model import User
from polaris.common import db
from polaris.analytics.db.model import Account, Organization
import uuid
from datetime import datetime

from polaris.utils.exceptions import ProcessingException


def create_account(company, owner_key=None, join_this=None):
    with db.orm_session(join_this) as session:
        organization = Organization(
            key=uuid.uuid4(),
            name=company,
            created=datetime.utcnow()
        )
        account = Account(
            key=uuid.uuid4(),
            name=company,
            owner_key=owner_key,
            created=datetime.utcnow()
        )
        account.organizations.append(organization)
        session.add(account)
        return account


def create_account_with_owner(company, account_owner_info):
    with db.orm_session() as session:
        user = User.find_by_email(session, account_owner_info.email)
        if user and 'account-owner' in user.role_names:
            raise ProcessingException(
                'User exists and is already an account owner on a different account.'
                'Cannot create a new account with this same owner'
            )

        account = create_account(
            company,
            owner_key=user.key if user else None,
            join_this=session
        )

        if not user:
            user = auth_db_api.create_user(
                **account_owner_info,
                account_key=account.key,
                role_name='account-owner',
                join_this=session
            )
            user.account_key = account.key
            account.owner_key = user.key
            session.add(user)

    return account
