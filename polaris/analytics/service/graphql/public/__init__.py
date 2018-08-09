# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from .selectables import PublicOrganizationsNodes, PublicProjectsNodes, PublicRepositoriesNodes
from ..organization import Organization
from ..project import Project
from ..repository import Repository


class Public(
    graphene.ObjectType
):
    # Child fields
    organizations = Organization.ConnectionField()
    projects = Project.ConnectionField()
    repositories = Repository.ConnectionField()

    @classmethod
    def Field(cls):
        return graphene.Field(cls)

    @classmethod
    def resolve_field(cls, info, **kwargs):
        return Public()


    def resolve_organizations(self, info, **kwargs):
        return Organization.resolve_connection(
            'public_organizations',
            PublicOrganizationsNodes,
            {},
            **kwargs
        )

    def resolve_projects(self, info, **kwargs):
        return Project.resolve_connection(
            'public_projects',
            PublicProjectsNodes,
            {},
            **kwargs
        )

    def resolve_repositories(self, info, **kwargs):
        return Repository.resolve_connection(
            'public_repositories',
            PublicRepositoriesNodes,
            {},
            **kwargs
        )
