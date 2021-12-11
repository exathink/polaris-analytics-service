# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging

import graphene
from flask import abort
from flask_login import current_user

from polaris.analytics import api
from polaris.analytics.service.graphql.input_types import AccountProfileInput, UserInfoInput, OrganizationInput
from polaris.analytics.service.invite import send_new_member_invite
from polaris.common import db
from polaris.utils.exceptions import ProcessingException
from .. import Account

logger = logging.getLogger('polaris.analytics.graphql')


class CreateAccountInput(graphene.InputObjectType):
    name = graphene.String(required=True)
    profile = graphene.Field(AccountProfileInput, required=False)

    default_organization = graphene.Field(OrganizationInput, required=True)
    account_owner_info = graphene.Field(UserInfoInput, required=True)


class CreateAccount(graphene.Mutation):
    class Arguments:
        create_account_input = CreateAccountInput(required=True)

    account = Account.Field()

    def mutate(self, info, create_account_input):

        if 'admin' in current_user.role_names:
            logger.info('Create Account called')

            with db.orm_session() as session:
                account, owner = api.create_account_with_owner_and_default_org(
                    account_info=create_account_input,
                    organization_info=create_account_input.default_organization,
                    account_owner_info=create_account_input.account_owner_info,
                    join_this=session
                )

                send_new_member_invite(owner, invitation=dict(subject=f'Welcome to the Polaris Advisor Program, {owner.first_name}'))
                if account is not None:
                    return CreateAccount(
                        account=Account.resolve_field(info, key=account.key)
                    )

                else:
                    raise ProcessingException("Account was not created")
        else:
            abort(403)


class AccountMutationsMixin:
    create_account = CreateAccount.Field()
