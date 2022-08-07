# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Priya Mukundan
import uuid

import pytest
from test.fixtures.users import *

from polaris.utils.collections import Fixture

from graphene.test import Client
from polaris.analytics.service.graphql import schema
import graphene
from polaris.common.enums import AccountRoles, OrganizationRoles
from unittest.mock import patch
from polaris.common import db

OrganizationRoleType = graphene.Enum.from_enum(OrganizationRoles)


class OrgRoleDictionary(graphene.InputObjectType):
    org_key = graphene.String()
    role = graphene.Field(OrganizationRoleType)


class TestUpdateUserMutation:

    @pytest.fixture()
    def setup(self, account_org_user_fixture):
        account, organizations, user = account_org_user_fixture

        mutation = """ 
            mutation updateUser($accountKey: String!, $key: String!, $accountRole: AccountRoles, $email: String
            , $firstName: String, $lastName: String, $organizationRoles: [OrgRoleDictionary], $active: Boolean)
                {updateUser(updateUserInput: {accountKey: $accountKey,
                key: $key
                accountRole: $accountRole,
                email: $email,
                firstName:$firstName,
                lastName: $lastName,
                organizationRoles:$organizationRoles
                active: $active

                  }) {
                    user{
                    key}   
                    updated
                  }
                }
        
        
        """
        yield Fixture(
            account=account,
            organizations=organizations,
            user=user,
            mutation=mutation
        )

    def it_updates_a_user(self, setup):
        fixture = setup

        client = Client(schema)
        with patch('polaris.analytics.service.graphql.viewer.Viewer.is_account_owner', return_value=True):
            result = client.execute(
                fixture.mutation,
                variable_values=dict(
                    accountKey=fixture.account.key,
                    key=fixture.user.key,
                    accountRole="member",
                    firstName="Elizabeth",
                    lastName="Bennett",
                    email="elizabeth.bennett@janeausten.com",
                    organizationRoles=[dict(orgKey=fixture.organizations[0].key, role="member"),
                                       dict(orgKey=fixture.organizations[1].key, role="member")],
                    active=True
                )
            )
        assert result['data']['updateUser']['updated']

    def it_updates_the_database(self, setup):
        fixture = setup

        client = Client(schema)

        with patch('polaris.analytics.service.graphql.viewer.Viewer.is_account_owner', return_value=True):
            result = client.execute(
                fixture.mutation,
                variable_values=dict(
                    accountKey=fixture.account.key,
                    key=fixture.user.key,
                    accountRole="member",
                    firstName="Elizabeth",
                    lastName="Bennett",
                    email="elizabeth.bennett@janeausten.com",
                    organizationRoles=[dict(orgKey=fixture.organizations[0].key, role="owner")],
                    active=True
                )
            )
        assert db.connection().execute(f"select id from auth.users where key='{fixture.user.key}' and first_name='Elizabeth'").scalar() is not None

        assert db.connection().execute(
            f"select id from analytics.accounts t1, analytics.account_members t2 where t1.id = t2.account_id and t1.key = '{fixture.account.key}' and  t2.user_key='{fixture.user.key}' and role = 'member'").scalar() is not None

        assert db.connection().execute(
            f"select id from analytics.organizations t1, analytics.organization_members t2 where t1.id = t2.organization_id and t1.key = '{fixture.organizations[0].key}' and  t2.user_key='{fixture.user.key}' and role = 'owner'").scalar() is not None

        assert db.connection().execute(
            f"select id from analytics.organizations t1, analytics.organization_members t2 where t1.id = t2.organization_id and t1.key = '{fixture.organizations[1].key}' and  t2.user_key='{fixture.user.key}' and role = 'owner'").scalar() is None

    def it_throws_an_error_message_when_account_is_invalid(self, setup):
        fixture = setup
        account_key = uuid.uuid4().hex
        client = Client(schema)
        with patch('polaris.analytics.service.graphql.viewer.Viewer.is_account_owner', return_value=True):
            result = client.execute(
                fixture.mutation,
                variable_values=dict(
                    accountKey=account_key,
                    key=fixture.user.key,
                    firstName="Elizabeth",
                    lastName="Bennett",
                    email="elizabeth.bennett@janeausten.com",
                    active=True
                )
            )
        assert 'errors' in result
        assert db.connection().execute(
            f"select id from auth.users where key='{fixture.user.key}' and first_name='Elizabeth'").scalar() is None
