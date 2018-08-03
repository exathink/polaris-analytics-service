# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from ..interfaces import NamedNode, CommitSummary, ContributorSummary
from ..mixins import NamedNodeResolverMixin, CommitSummaryResolverMixin, ContributorSummaryResolverMixin

from ..repository import Repository
from .selectables import ProjectNode, ProjectsContributorSummary, ProjectsCommitSummary, \
    ProjectRepositoriesNodes

from polaris.graphql.connection_utils import CountableConnection, QueryConnectionField


class Project(
    NamedNodeResolverMixin,
    CommitSummaryResolverMixin,
    ContributorSummaryResolverMixin,
    graphene.ObjectType
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorSummary)

    NamedNodeResolver = ProjectNode
    InterfaceResolvers = {
        'CommitSummary': ProjectsCommitSummary,
        'ContributorSummary': ProjectsContributorSummary
    }
    InterfaceEnum = graphene.Enum(
        'ProjectInterfaces', [
            ('CommitSummary', 'CommitSummary'),
            ('ContributorSummary', 'ContributorSummary')
        ]
    )

    # Child Fields
    repositories = Repository.ConnectionField()

    @classmethod
    def Field(cls):
        return graphene.Field(
            cls,
            key=graphene.Argument(type=graphene.String, required=True),
            interfaces=graphene.Argument(
                graphene.List(cls.InterfaceEnum),
                required=False,
            )
        )

    @classmethod
    def ConnectionField(cls, **kwargs):
        return QueryConnectionField(
            Projects,
            interfaces=graphene.Argument(
                graphene.List(cls.InterfaceEnum),
                required=False,
            ),
            **kwargs
        )

    @classmethod
    def resolve_field(cls, parent, info, project_key, **kwargs):
        return cls.resolve_instance(params=dict(project_key=project_key), **kwargs)

    def get_node_query_params(self):
        return dict(project_key=self.key)

    def resolve_repositories(self, info, **kwargs):
        return Repository.resolve_connection(
            'project_repositories',
            ProjectRepositoriesNodes,
            self.get_node_query_params(),
            **kwargs
        )


class Projects(CountableConnection):
    class Meta:
        node = Project
