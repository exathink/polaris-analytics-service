# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from .project_repositories import ProjectRepositories
from .project_commit_summary import ProjectCommitSummary
from ..utils import resolve_instance

from ..interfaces import NamedNode, CommitSummary, ContributorSummary
from polaris.analytics.service.graphql.mixins import \
    CommitSummaryResolverMixin, \
    ContributorSummaryResolverMixin

from polaris.common import db
from polaris.repos.db.model import Project as ProjectModel
from .selectables import ProjectNode, ProjectsContributorSummary, ProjectsCommitSummary


class Project(
    CommitSummaryResolverMixin,
    ContributorSummaryResolverMixin,
    graphene.ObjectType
):
    class Meta:
        interfaces = (NamedNode, CommitSummary, ContributorSummary)

    repositories = graphene.Field(ProjectRepositories)

    @classmethod
    def Field(cls):
        return graphene.Field(
            cls,
            key=graphene.Argument(type=graphene.String, required=True),
            interfaces=graphene.Argument(
                graphene.List(graphene.Enum(
                    'ProjectInterfaces', [
                        ('CommitSummary', 'CommitSummary'),
                        ('ContributorSummary', 'ContributorSummary')
                    ]
                )),
                required=False,
            )
        )

    InterfaceResolvers = {
        'NamedNode': ProjectNode,
        'CommitSummary': ProjectsCommitSummary,
        'ContributorSummary': ProjectsContributorSummary
    }

    @classmethod
    def resolve_field(cls, parent, info, project_key, **kwargs):
        return resolve_instance(
            cls.InterfaceResolvers,
            resolver_context='project',
            params=dict(project_key=project_key),
            output_type=cls,
            **kwargs
        )

    def get_node_query_params(self):
        return dict(project_key=self.key)

    def resolve_repositories(self, info, **kwargs):
        return ProjectRepositories.resolve(self, info, **kwargs)



