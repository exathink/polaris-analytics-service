# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from flask_security import current_user

from graphene import relay
from ..interfaces import CommitSummary

from .commit_summary import AccountCommitSummary

from .enums import AccountPartitions


class Account(graphene.ObjectType):
    class Meta:
        interfaces = (relay.Node, )

    commit_summary = graphene.Field(
        graphene.List(AccountCommitSummary),
        group_by=graphene.Argument(type=AccountPartitions, required=False, default_value=AccountPartitions.account)
    )

    @classmethod
    def resolve_field(cls, parent, info, **kwargs):
        return Account(id=current_user.account_key)


    def resolve_commit_summary(self, info, **kwargs):
            return AccountCommitSummary.resolve(self, info, **kwargs)

