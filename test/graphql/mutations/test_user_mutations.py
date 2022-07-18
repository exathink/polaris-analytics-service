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

OrganizationRoleType = graphene.Enum.from_enum(OrganizationRoles)


class OrgRoleDictionary(graphene.InputObjectType):
    org_key = graphene.String()
    role = graphene.Field(OrganizationRoleType)


class TestUpdateUser:

    @pytest.fixture()
    def setup(self, account_org_user_fixture):
        account, organization, user = account_org_user_fixture

        mutation = """ 
            mutation updateUser($accountKey: String!, $key: String!, $accountRole: AccountRoles, $email: String
            , $firstName: String, $lastName: String, $organizationRoles: [OrgRoleDictionary])
                {updateUser(updateUserInput: {accountKey: $accountKey,
                key: $key
                accountRole: $accountRole,
                email: $email,
                firstName:$firstName,
                lastName: $lastName,
                organizationRoles:$organizationRoles

                  }) {
                    user{
                    key}   
                    updated
                  }
                }
        
        
        """
        yield Fixture(
            account=account,
            organization=organization,
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
                    firstName="Elizabeth",
                    lastName="Bennett",
                    email="elizabeth.bennett@janeausten.com",
                )
            )
        assert result['data']['updateUser']['updated']


