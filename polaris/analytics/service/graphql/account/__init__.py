# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene


from flask_security import current_user

from ..interfaces import CommitSummary, NamedNode
from polaris.analytics.service.graphql.utils import AccessDeniedException
from polaris.analytics.service.graphql.mixins import \
    NamedNodeResolverMixin, \
    CommitSummaryResolverMixin, \
    ContributorSummaryResolverMixin

from .account_resolvers import *

from .account_organizations import AccountOrganizations
from .account_repositories import AccountRepositories
from .account_projects import AccountProjects

from ..utils import resolve_join, collect_join_resolvers

from polaris.common import db

from polaris.repos.db.model import Account as AccountModel



class Account(
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    ContributorSummaryResolverMixin,
    graphene.ObjectType
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorSummary)

    organizations = AccountOrganizations.Field()
    projects = graphene.Field(AccountProjects)
    repositories = graphene.Field(AccountRepositories)

    @classmethod
    def Field(cls):
        return graphene.Field(
            Account,
            key=graphene.Argument(type=graphene.String, required=True),
            interfaces=graphene.Argument(
                graphene.List(graphene.Enum(
                    'AccountInterfaces', [
                        ('CommitSummary', 'CommitSummary'),
                        ('ContributorSummary', 'ContributorSummary')
                    ]
                )),
                required=False,
            )
        )

    InterfaceResolvers = {
        'NamedNode': AccountNode,
        'CommitSummary': AccountCommitSummary,
        'ContributorSummary': AccountContributorSummary
    }

    @classmethod
    def resolve_field(cls, info, key, **kwargs):
        if key == str(current_user.account_key):
            resolvers = collect_join_resolvers(cls.InterfaceResolvers, **kwargs)
            resolved = resolve_join(resolvers, resolver_context='account', output_type=Account,
                                params=dict(account_key=key), **kwargs)
            return resolved[0] if len(resolved) == 1 else None
        else:
            raise AccessDeniedException('Access denied for specified account')


    @staticmethod
    def load_instance(key, info, **kwargs):
        with db.orm_session() as session:
            return AccountModel.find_by_account_key(session, key)

    def get_node_query_params(self):
        return dict(account_key=self.key)

    def resolve_organizations(self, info, **kwargs):
        return AccountOrganizations.resolve(self, info, **kwargs)

    def resolve_projects(self, info, **kwargs):
        return AccountProjects.resolve(self, info, **kwargs)

    def resolve_repositories(self, info, **kwargs):
        return AccountRepositories.resolve(self, info, **kwargs)





