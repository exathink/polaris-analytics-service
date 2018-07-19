# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene



from .commit_summary_for_organization import CommitSummaryForOrganization
from .commit_summary_by_project import OrganizationCommitSummaryByProject
from .commit_summary_by_repository import OrganizationCommitSummaryByRepository


class OrganizationCommitSummary(graphene.ObjectType):

    def __init__(self, organization, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.organization = organization

    for_organization = graphene.Field(CommitSummaryForOrganization)
    by_project = graphene.Field(graphene.List(OrganizationCommitSummaryByProject))
    by_repository = graphene.Field(graphene.List(OrganizationCommitSummaryByRepository))


    @classmethod
    def Field(cls):
        return graphene.Field(OrganizationCommitSummary)

    @classmethod
    def resolve(cls, organization, info, **kwargs):
        return OrganizationCommitSummary(organization)


    def resolve_for_organization(self, info, **kwargs):
        return CommitSummaryForOrganization.resolve(self.organization.id, info, **kwargs)


    def resolve_by_project(self, info, **kwargs):
        return OrganizationCommitSummaryByProject.resolve(self.organization.id, info, **kwargs)



    def resolve_by_repository(self, info, **kwargs):
        return OrganizationCommitSummaryByRepository.resolve(self.organization_key, info, **kwargs)



