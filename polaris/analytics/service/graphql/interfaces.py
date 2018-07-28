# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from graphene.relay import Node
from collections import namedtuple

class GQLException(Exception):
    pass

class AccessDeniedException(GQLException):
    pass

class NamedNode(Node):
    key = graphene.String(required=True)
    name = graphene.String(required=True)


class CommitSummary(Node):
    Impl = namedtuple('CommitSummary', 'key earliest_commit latest_commit commit_count')

    earliest_commit = graphene.DateTime(required=True)
    latest_commit = graphene.DateTime(required=True)
    commit_count = graphene.Int(default_value=0)
    contributor_count = graphene.Int(default_value=0)

    @classmethod
    def UnResolved(cls):
        return cls.Impl(None, None, None, None, None)


class ContributorSummary(Node):
    Impl = namedtuple('ContributorSummary', 'key contributor_count')

    contributor_count = graphene.Int(default_value=0)

    @classmethod
    def UnResolved(cls):
        return cls.Impl(None, None)
