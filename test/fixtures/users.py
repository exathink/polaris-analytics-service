# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Priya Mukundan


import uuid
import pytest

from polaris.analytics.db.model import Account, Organization
from polaris.auth.db.model import User

from polaris.common import db
from polaris.common.enums import AccountRoles, OrganizationRoles


test_account_key = uuid.uuid4().hex
test_organization_key = uuid.uuid4().hex
test_user_key = uuid.uuid4().hex

@pytest.fixture()
def account_org_user_fixture(setup_schema):
    with db.orm_session() as session:
        session.expire_on_commit = False
        account = Account(key=test_account_key,
                          name='test-account')

        organization = Organization(
            key=test_organization_key,
            name='test-org',
            public=False
        )

        user = User(key=test_user_key,
                    email="Emma.Woodhouse@janeausten.com",
                    first_name="Emma",
                    last_name="Woodhouse",
                    account_key=test_account_key,
                    active=True,
                    password="ohmyohmy")

        account.organizations.append(organization)
        account.add_member(user, AccountRoles.owner)
        account.set_owner(user)
        organization.add_member(user, OrganizationRoles.member)
        session.add(user)
        session.add(organization)
        session.add(account)

        session.flush()

    yield account, organization, user

    db.connection().execute("delete from analytics.accounts_organizations")
    db.connection().execute("delete from analytics.organization_members")
    db.connection().execute("delete from analytics.account_members")
    db.connection().execute("delete from auth.users")
    db.connection().execute("delete from analytics.organizations")
    db.connection().execute("delete from analytics.accounts")


@pytest.fixture()
def cleanup():
    yield

