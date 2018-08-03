# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene



class CommitSummary(graphene.Interface):
    earliest_commit = graphene.DateTime(required=True)
    latest_commit = graphene.DateTime(required=True)
    commit_count = graphene.Int(required=True)

class ContributorSummary(graphene.Interface):
    unassigned_alias_count = graphene.Int(required=True)
    unique_contributor_count = graphene.Int(required=True)
    contributor_count = graphene.Int(required=True)


