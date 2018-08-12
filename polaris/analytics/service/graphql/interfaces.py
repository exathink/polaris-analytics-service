# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene


class CommitSummary(graphene.Interface):
    earliest_commit = graphene.DateTime(required=False)
    latest_commit = graphene.DateTime(required=False)
    commit_count = graphene.Int(required=False, default_value=0)


class ContributorSummary(graphene.Interface):
    unassigned_alias_count = graphene.Int(required=False)
    unique_contributor_count = graphene.Int(required=False)
    contributor_count = graphene.Int(required=False, default_value=0)

class ProjectCount(graphene.Interface):
    project_count = graphene.Int(required=False, default_value=0)

class RepositoryCount(graphene.Interface):
    repository_count = graphene.Int(required=False, default_value=0)

class OrganizationRef(graphene.Interface):
    organization_name = graphene.String(required=True)
    organization_key = graphene.String(required=True)

