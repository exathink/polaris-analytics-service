# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from graphene import relay

from .commit_summary import OrganizationCommitSummary


class Organization(graphene.ObjectType):
    class Meta:
        interfaces = (relay.Node, )

    commit_summary = OrganizationCommitSummary.Field()

    @classmethod
    def resolve_field(cls, parent, info, organization_key, **kwargs):
        return Organization(id=organization_key)



    def resolve_commit_summary(self, info, **kwargs):
            return OrganizationCommitSummary.resolve(self, info, **kwargs)

