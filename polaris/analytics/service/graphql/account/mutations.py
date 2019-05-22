# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
import graphene

from polaris.analytics import api
from polaris.auth.db.model import User
from polaris.auth.db import api as auth_db_api
from polaris.common import db

from polaris.utils.exceptions import ProcessingException
from ..account.interfaces import AccountProfile
from flask import abort
from flask_login import current_user
from .. import Account

logger = logging.getLogger('polaris.analytics.graphql')


class UserInfoInput(graphene.InputObjectType):
    first_name = graphene.String(required=True)
    last_name = graphene.String(required=True)
    email = graphene.String(required=True)


class CreateAccountInput(graphene.InputObjectType):
    company = graphene.String(required=True)
    account_owner_info = graphene.Field(UserInfoInput, required=False)
    account_profile = graphene.Field(AccountProfile, required=False)


class CreateAccount(graphene.Mutation):
    class Arguments:
        create_account_input = CreateAccountInput(required=True)

    account = Account.Field()

    def mutate(self, info, create_account_input):

        if 'admin' in current_user.role_names:
            logger.info('Creat Account called')
            with db.orm_session() as session:
                user = User.find_by_email(session, create_account_input.account_owner_info.email)
                if user and 'account-owner' in user.role_names:
                    raise ProcessingException(
                        'User exists and is already an account owner on a different account.'
                        'Cannot create a new account with this same owner'
                    )

                account = api.create_account(
                    create_account_input.company,
                    owner_key=user.key if user else None,
                    join_this=session
                )

                if not user:
                    user = auth_db_api.create_user(
                        **create_account_input.account_owner_info,
                        account_key=account.key,
                        role_name='account-owner',
                        join_this=session
                    )
                    user.account_key = account.key
                    account.owner_key = user.key
                    session.add(user)

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
