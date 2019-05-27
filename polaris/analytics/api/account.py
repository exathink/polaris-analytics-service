# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import uuid
from polaris.auth.db import api as auth_db_api
from polaris.auth.db.model import User
from polaris.common import db
from polaris.analytics.db.model import Account, Organization, AccountMember, \
    OrganizationMember, AccountRoles, OrganizationRoles

from datetime import datetime

from polaris.utils.exceptions import ProcessingException


def create_account_and_organization(company, owner_key=None, join_this=None):
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
        return account, organization


def create_account_with_owner(company, account_owner_info, join_this=None):
    with db.orm_session(join_this) as session:
        if Account.find_by_name(session, company) is None:
            user = User.find_by_email(session, account_owner_info.email)
            if user and 'account-owner' in user.role_names:
                raise ProcessingException(
                    'User exists and is already an account owner on a different account.'
                    'Cannot create a new account with this same owner'
                )

            account, organization = create_account_and_organization(
                company,
                owner_key=user.key if user else None,
                join_this=session
            )

            if not user:
                user = auth_db_api.create_user(
                    account_key=account.key,
                    **account_owner_info,
                    join_this=session
                )
                user.account_key = account.key
                account.owner_key = user.key
                session.add(user)

            account.members.append(
                AccountMember(
                    user_key=user.key,
                    role=AccountRoles.owner.value
                )
            )

            organization.members.append(
                OrganizationMember(
                    user_key=user.key,
                    role=OrganizationRoles.owner.value
                )
            )

            return account, user
        else:
            raise ProcessingException('An account with this name already exists')
