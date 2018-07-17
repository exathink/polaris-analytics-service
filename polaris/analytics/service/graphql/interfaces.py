# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

class NamedNode(graphene.relay.Node):
    atype = graphene.String(required=True)
    key = graphene.String(required=True)
    name = graphene.String(required=True)



class KeyIdResolverMixin:
    def resolve_id(self, info, **kwargs):
        return f"{self.atype}:{self.key}"

class CommitSummary(graphene.Interface):


    earliest_commit = graphene.DateTime(required=True)
    latest_commit = graphene.DateTime(required=True)
    commit_count = graphene.Int(default_value=0)
    contributor_count = graphene.Int()
