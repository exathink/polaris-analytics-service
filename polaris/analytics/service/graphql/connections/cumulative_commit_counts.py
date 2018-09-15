# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from polaris.graphql.connection_utils import CountableConnection
from polaris.graphql.interfaces import NamedNode


class CumulativeCommitCount(NamedNode):
    year = graphene.Int(required=True)
    month = graphene.Int(required=False)
    week = graphene.Int(required=False)
    commit_count = graphene.Int(required=True)


class CumulativeCommitCountConnection(
    CountableConnection
):
    class Meta:
        node = CumulativeCommitCount



