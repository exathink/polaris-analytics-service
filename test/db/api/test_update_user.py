# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Priya Mukundan
import uuid
from types import SimpleNamespace

import pytest
from flask import json
from test.fixtures.users import *

from polaris.utils.collections import Fixture, dict_to_object

from graphene.test import Client
from polaris.analytics.service.graphql import schema
from polaris.analytics import api
import graphene
from polaris.common.enums import AccountRoles, OrganizationRoles
from unittest.mock import patch
from polaris.common import db
from polaris.analytics.api import update_user

AccountRoleType = graphene.Enum.from_enum(AccountRoles)
OrganizationRoleType = graphene.Enum.from_enum(OrganizationRoles)


class OrgRoleDictionary(graphene.InputObjectType):
    org_key = graphene.String()
    role = graphene.Field(OrganizationRoleType)


class TestUpdateUser:

    @pytest.fixture()
    def setup(self, account_org_user_fixture):
        account, organizations, user = account_org_user_fixture

        yield Fixture(
            account=account,
            organizations=organizations,
            user=user
        )

    def it_updates_a_user(self, setup):
        fixture = setup

        org_role1 = OrgRoleDictionary()
        org_role1.org_key = fixture.organizations[0].key
        org_role1.role = "member"

        org_role2 = OrgRoleDictionary()
        org_role2.org_key = fixture.organizations[1].key
        org_role2.role = "member"

        organization_roles = [org_role1, org_role2]

        update_user_input = dict_to_object(dict(account_key=fixture.account.key,
                                                key=fixture.user.key,
                                                account_role="member",
                                                first_name="Elizabeth",
                                                last_name="Bennett",
                                                email="Elizabeth.Bennett@janepausten.com",
                                                active=True,
                                                organization_roles=organization_roles
                                                ))

        with db.orm_session() as session:
            user, updated, account = api.update_user(update_user_input,
                                                     join_this=session)

        assert updated

    def it_updates_the_database(self, setup):
        fixture = setup

        org_role1 = OrgRoleDictionary()
        org_role1.org_key = fixture.organizations[0].key
        org_role1.role = "member"

        org_role2 = OrgRoleDictionary()
        org_role2.org_key = fixture.organizations[1].key
        org_role2.role = "member"

        organization_roles = [org_role1, org_role2]

        update_user_input = dict_to_object(dict(account_key=fixture.account.key,
                                                key=fixture.user.key,
                                                account_role="member",
                                                first_name="Elizabeth",
                                                last_name="Bennett",
                                                email="Elizabeth.Bennett@janepausten.com",
                                                active=True,
                                                organization_roles=organization_roles
                                                ))

        with db.orm_session() as session:
            user, updated, account = api.update_user(update_user_input,
                                                     join_this=session)

        assert db.connection().execute(
            f"select id from auth.users where key='{fixture.user.key}' and first_name='Elizabeth'").scalar() is not None

    def it_throws_an_error_message_when_account_is_invalid(self, setup):
        fixture = setup
        account_key = uuid.uuid4().hex

        org_role1 = OrgRoleDictionary()
        org_role1.org_key = fixture.organizations[0].key
        org_role1.role = "member"

        org_role2 = OrgRoleDictionary()
        org_role2.org_key = fixture.organizations[1].key
        org_role2.role = "member"

        organization_roles = [org_role1, org_role2]

        update_user_input = dict_to_object(dict(account_key=account_key,
                                                key=fixture.user.key,
                                                account_role="member",
                                                first_name="Elizabeth",
                                                last_name="Bennett",
                                                email="Elizabeth.Bennett@janepausten.com",
                                                active=True,
                                                organization_roles=organization_roles
                                                ))

        with db.orm_session() as session:
            try:
                user, updated, account = api.update_user(update_user_input,
                                         join_this=session)
                assert False
            except Exception as excinfo:
                assert "Account with key" in excinfo.args[0]

        assert db.connection().execute(
            f"select id from auth.users where key='{fixture.user.key}' and first_name='Elizabeth'").scalar() is None

    def it_throws_an_error_message_when_organization_is_invalid(self, setup):
        fixture = setup

        org_role1 = OrgRoleDictionary()
        org_role1.org_key = uuid.uuid4().hex
        org_role1.role = "member"

        organization_roles = [org_role1]

        update_user_input = dict_to_object(dict(account_key=fixture.account.key,
                                                key=fixture.user.key,
                                                account_role="member",
                                                first_name="Elizabeth",
                                                last_name="Bennett",
                                                email="Elizabeth.Bennett@janepausten.com",
                                                active=True,
                                                organization_roles=organization_roles
                                                ))

        with db.orm_session() as session:
            try:
                user, updated, account = api.update_user(update_user_input,
                                                         join_this=session)
                assert False
            except Exception as excinfo:
                assert "Organization with key" in excinfo.args[0]

        assert db.connection().execute(
            f"select id from auth.users where key='{fixture.user.key}' and first_name='Elizabeth'").scalar() is None


    def it_throws_an_error_message_when_user_is_invalid(self, setup):
        fixture = setup
        user_key = uuid.uuid4().hex

        org_role1 = OrgRoleDictionary()
        org_role1.org_key = fixture.organizations[0].key
        org_role1.role = "member"

        org_role2 = OrgRoleDictionary()
        org_role2.org_key = fixture.organizations[1].key
        org_role2.role = "member"

        organization_roles = [org_role1, org_role2]

        update_user_input = dict_to_object(dict(account_key=fixture.account.key,
                                                key=user_key,
                                                account_role="member",
                                                first_name="Elizabeth",
                                                last_name="Bennett",
                                                email="Elizabeth.Bennett@janepausten.com",
                                                active=True,
                                                organization_roles=organization_roles
                                                ))

        with db.orm_session() as session:
            try:
                user, updated, account = api.update_user(update_user_input,
                                                         join_this=session)
                assert False
            except Exception as excinfo:
                assert "User with key" in excinfo.args[0]
