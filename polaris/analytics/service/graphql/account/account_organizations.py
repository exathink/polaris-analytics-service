# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import graphene
from ..organization import Organization
from .account_organizations_nodes import AccountOrganizationsNodes
from .account_organizations_commit_summaries import AccountOrganizationsCommitSummaries
from .account_organizations_count import AccountOrganizationsCount

from ..interfaces import *

from ..mixins import RemoteJoinResolverMixin
from .account_organizations_contributor_summaries import AccountOrganizationsContributorSummaries

from ..utils import resolve_local_join, resolve_remote_join, resolve_cte_join


class AccountOrganizations(
    RemoteJoinResolverMixin,
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

    NodeInterfaceResolvers = {
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

    def resolve_nodes(self, info, **kwargs):
        queries = self.collect_cte_resolve_queries(info, **kwargs)
        return resolve_cte_join(queries, output_type=Organization, params=dict(account_key=self.account.key))

    def resolve_count(self, info, **kwargs):
        return AccountOrganizationsCount.resolve(self.account.key, info, **kwargs)
