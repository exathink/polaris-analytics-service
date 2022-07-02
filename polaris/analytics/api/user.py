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


def update_user(account_key, key, account_role, active, email, first_name, last_name, organization_roles, join_this=None):
    with db.orm_session(join_this) as session:
        added_orgs = []
        updated = False
        account = Account.find_by_account_key(session, account_key)
        if account is not None:

            user = User.find_by_key(session, key)
            if user is not None:
                if active is not None or email is not None or first_name is not None or last_name is not None:
                    user_data_to_update = {}
                    if active is not None:
                        user_data_to_update['active'] = active
                    if email is not None:
                        user_data_to_update['email'] = email
                    if first_name is not None:
                        user_data_to_update['first_name'] = first_name
                    if last_name is not None:
                        user_data_to_update['last_name'] = last_name

                    user.update(user_data_to_update)
                    updated = True

                if account_role is not None:
                    if account_role in [member.name for member in AccountRoles]:
                        account_member = account.get_member(user)
                        setattr(account_member, 'role', account_role)
                        updated = True
                    else:
                        raise ProcessingException(f'Account Role provided  {account_role} is invalid')

                if organization_roles is not None:
                    for org_role in organization_roles:
                        if org_role.role in [member.name for member in OrganizationRoles]:
                            organization = Organization.find_by_organization_key(session, org_role.org_key)
                            if organization.belongs_to_account(account):
                                organization_member = organization.get_member(user)
                                if organization_member is not None:
                                    setattr(organization_member, 'role', org_role.role)
                                else:
                                    if organization.add_member(user):
                                        added_orgs.append(organization)
                                updated = True
                            else:
                                raise ProcessingException(f'Organization with key {org_role.org_key} does not belong '
                                                          f'to account with account key {account_key}')
                        else:
                            raise ProcessingException(f'Organization Role provided  {org_role.role} is invalid')
                return user, updated,  account, added_orgs
            else:
                raise ProcessingException(f'User with key {key} not found')
        else:
            raise ProcessingException(f'Account with key {account_key} not found')
