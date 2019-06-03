# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.analytics.db.model import Account
from polaris.auth.db import api as auth_db_api
from polaris.auth.db.model import User
from polaris.common import db
from polaris.utils.exceptions import ProcessingException


def create_account_with_owner_and_default_org(account_info, organization_info, account_owner_info, join_this=None):
    with db.orm_session(join_this) as session:
        if Account.find_by_name(session, account_info.name) is None:
            user = User.find_by_email(session, account_owner_info.email)
            account = Account.create(
                name=account_info.name,
                profile=account_info.profile
            )

            if not user:
                user = auth_db_api.create_user(
                    account_key=account.key,
                    **account_owner_info,
                    join_this=session
                )
                user.account_key = account.key
                session.add(user)

            account.set_owner(user)

            account.create_organization(
                name=organization_info.name,
                profile=organization_info.profile,
                owner=user
            )

            session.add(account)

            return account, user
        else:
            raise ProcessingException('An account with this name already exists')
