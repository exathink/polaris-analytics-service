# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
from flask import abort
from flask_login import current_user
import graphene
from polaris.common import db
from polaris.analytics import api
from polaris.analytics.service.invite import send_new_member_invite, send_join_account_notice

from ..viewer import Viewer
from . import User

logger = logging.getLogger('polaris.analytics.graphql')


class InviteUserInput(graphene.InputObjectType):
    account_key = graphene.String(required=True)
    email = graphene.String(required=True)
    first_name = graphene.String(required=True)
    last_name = graphene.String(required=True)
    organizations = graphene.Field(graphene.List(graphene.String), required=True)


class InviteUser(graphene.Mutation):
    class Arguments:
        invite_user_input = InviteUserInput(required=True)

    user = User.Field()
    created = graphene.Boolean()
    invite_sent = graphene.Boolean()


    def mutate(self, info, invite_user_input):
        if Viewer.is_account_owner(invite_user_input.account_key):
            with db.orm_session() as session:
                user, created, added, account, added_orgs = api.invite_user(
                    invite_user_input.email,
                    invite_user_input.first_name,
                    invite_user_input.last_name,
                    invite_user_input.account_key,
                    invite_user_input.organizations,
                    join_this=session
                )
                invite_sent = False
                if user is not None:
                    if created:
                        invite_sent = send_new_member_invite(user, invitation=dict(
                            subject=f"{current_user.first_name} {current_user.last_name} has invited you to join Urjuna"
                        ))
                    elif added and len(added_orgs) > 0:
                        invite_sent = send_join_account_notice(user, invitation=dict(
                            subject=f"{current_user.first_name} {current_user.last_name}"
                                     f" has added you to the organization {added_orgs[0].name} "
                                     f" on Urjuna"

                        ))
                    elif added:
                        invite_sent = send_join_account_notice(user, invitation=dict(
                            subject=f"{current_user.first_name} {current_user.last_name}"
                                    f" has added you to the account {account.name} "
                                    f" on Urjuna"

                        ))


                    return InviteUser(
                        user=User.resolve_field(info, user_key=user.key),
                        created=created,
                        invite_sent=invite_sent
                    )

        else:
            abort(403)


class UseMutationsMixin:
    invite_user = InviteUser.Field()
