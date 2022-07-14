# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.common import db
from polaris.utils.exceptions import ProcessingException
from polaris.auth.db.api import create_user
from polaris.auth.db.model import User
from polaris.analytics.db.model import Account, Organization, OrganizationMember, AccountMember
from polaris.common.enums import AccountRoles, OrganizationRoles


def invite_user(email, first_name, last_name, account_key, organization_keys, join_this=None):
    with db.orm_session(join_this) as session:
        created = False
        added = False
        added_orgs = []
        account = Account.find_by_account_key(session, account_key)
        if account is not None:

            user = User.find_by_email(session, email)
            if not user:
                user = create_user(
                    account_key=account_key,
                    email=email,
                    first_name=first_name,
                    last_name=last_name,
                    join_this=session
                )
                session.add(user)
                session.flush()
                created = True

            added = account.add_member(user)

            for organization_key in organization_keys:
                organization = Organization.find_by_organization_key(session, organization_key)
                if organization.belongs_to_account(account):
                    if organization.add_member(user):
                        added_orgs.append(organization)
                        added = True

            return user, created, added, account, added_orgs

        else:
            raise ProcessingException(f'Account with key {account_key} not found')


def update_user(user_info, join_this=None):
    with db.orm_session(join_this) as session:
        updated = False
        account = Account.find_by_account_key(session, user_info.account_key)
        if account is not None:

            user = User.find_by_key(session, user_info.key)
            if user is not None:
                user.update(user_info)
                updated = True

                if user_info.account_role is not None:
                    account.set_user_role(user, user_info.account_role)
                    updated = True

                if user_info.organization_roles is not None:
                    for org_role in user_info.organization_roles:
                        organization = Organization.find_by_organization_key(session, org_role.org_key)
                        if organization is not None:
                            if organization.belongs_to_account(account):
                                organization.set_user_role(user, org_role.role)
                                updated = True
                            else:
                                raise ProcessingException(f'Organization with key {org_role.org_key} does not '
                                                          f'belong to account with account key {account_key}')
                        else:
                            raise ProcessingException(f'Organization with key {org_role.org_key} does not exist')

                return user, updated,  account
            else:
                raise ProcessingException(f'User with key {key} not found')
        else:
            raise ProcessingException(f'Account with key {account_key} not found')
