# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import graphene

from .account_organizations_resolvers import *
from ..mixins import JoinResolverMixin, NamedNodeCountResolverMixin
from ..organization import Organization
from ..utils import resolve_join


class AccountOrganizations(
    NamedNodeCountResolverMixin,
    JoinResolverMixin,
    graphene.ObjectType
):

    def __init__(self, account, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.account = account


    count = graphene.Field(graphene.Int)
    nodes = graphene.Field(
        graphene.List(Organization),
        interfaces=graphene.Argument(
            graphene.List(graphene.Enum(
                'AccountOrganizationsInterfaces', [
                    ('CommitSummary', 'CommitSummary'),
                    ('ContributorSummary', 'ContributorSummary')
                ]
            )),
            required=False,

        )
    )

    InterfaceResolvers = {
        'NamedNode': AccountOrganizationsNodes,
        'CommitSummary': AccountOrganizationsCommitSummaries,
        'ContributorSummary': AccountOrganizationsContributorSummaries
    }

    @classmethod
    def Field(cls):
        return graphene.Field(AccountOrganizations)

    @classmethod
    def resolve(cls, account, info, **kwargs):
        return AccountOrganizations(account=account)

    def get_nodes_query_params(self):
        return dict(account_key=self.account.key)

    def resolve_nodes(self, info, **kwargs):
        resolvers = self.collect_join_resolvers(info, **kwargs)
        return resolve_join(resolvers, resolver_context='account_organizations', output_type=Organization,  params=self.get_nodes_query_params(), **kwargs)





