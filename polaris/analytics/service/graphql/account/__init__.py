# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from flask_security import current_user

from polaris.graphql.exceptions import AccessDeniedException
from polaris.graphql.interfaces import NamedNode
from polaris.graphql.selectable import Selectable
from .selectables import AccountNode, AccountCommitSummary, AccountContributorCount, AccountOrganizationsNodes, \
    AccountProjectsNodes, AccountRepositoriesNodes, AccountContributorNodes, AccountRecentlyActiveRepositoriesNodes
from ..contributor import ContributorsConnectionMixin
from ..interface_mixins import NamedNodeResolverMixin, CommitSummaryResolverMixin, ContributorCountResolverMixin
from ..interfaces import CommitSummary, ContributorCount
from ..organization import OrganizationsConnectionMixin
from ..project import ProjectsConnectionMixin
from ..repository import RepositoriesConnectionMixin, RecentlyActiveRepositoriesConnectionMixin


class Account(
    # Interface Mixins
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    ContributorCountResolverMixin,
    # ConnectionMixins
    OrganizationsConnectionMixin,
    ProjectsConnectionMixin,
    RepositoriesConnectionMixin,
    RecentlyActiveRepositoriesConnectionMixin,
    ContributorsConnectionMixin,
    #
    Selectable
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorCount)
        named_node_resolver = AccountNode
        interface_resolvers = {
            'CommitSummary': AccountCommitSummary,
            'ContributorCount': AccountContributorCount
        }
        connection_node_resolvers = {
            'organizations': AccountOrganizationsNodes,
            'projects': AccountProjectsNodes,
            'repositories': AccountRepositoriesNodes,
            'recently_active_repositories': AccountRecentlyActiveRepositoriesNodes,
            'contributors': AccountContributorNodes
        }

    @classmethod
    def resolve_field(cls, info, key, **kwargs):
        if key == str(current_user.account_key):
            return cls.resolve_instance(key, **kwargs)
        else:
            raise AccessDeniedException('Access denied for specified account')
