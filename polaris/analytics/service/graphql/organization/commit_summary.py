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
from .enums import OrganizationPartitions

class OrganizationCommitSummary(graphene.Union):

    class Meta:
        types = (
            CommitSummaryForOrganization,
            OrganizationCommitSummaryByProject,
            OrganizationCommitSummaryByRepository
        )
    @classmethod
    def Field(cls):
        return graphene.Field(
        graphene.List(OrganizationCommitSummary),
        group_by=graphene.Argument(type=OrganizationPartitions, required=False, default_value=OrganizationPartitions.organization.value)
    )

    @classmethod
    def resolve_type(cls, instance, info):
        if instance.type == OrganizationPartitions.organization.value:
            return CommitSummaryForOrganization
        elif instance.type == OrganizationPartitions.project.value:
            return OrganizationCommitSummaryByProject
        elif instance.type == OrganizationPartitions.repository.value:
            return OrganizationCommitSummaryByRepository


    @classmethod
    def resolve(cls, organization, info, **kwargs):
        if kwargs.get('group_by') == OrganizationPartitions.organization:
            return CommitSummaryForOrganization.resolve(organization, info, **kwargs)
        elif kwargs.get('group_by') == OrganizationPartitions.project:
            return OrganizationCommitSummaryByProject.resolve(organization, info, **kwargs)
        elif kwargs.get('group_by') == OrganizationPartitions.repository:
            return OrganizationCommitSummaryByRepository.resolve(organization, info, **kwargs)










