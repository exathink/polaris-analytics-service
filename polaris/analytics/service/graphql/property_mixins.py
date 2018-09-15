# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene

from polaris.graphql.selectable import SimpleSelectableResolverMixin
from .properties import CumulativeCommitCountProperty
from .interface_mixins import KeyIdResolverMixin


class SelectablePropertyResolverMixin(KeyIdResolverMixin, SimpleSelectableResolverMixin):

    def resolve_property(self, property_name, **kwargs):
        selectable = self._meta.property_resolvers.get(property_name)
        return self.resolve_selectable(selectable(**kwargs), self.get_instance_query_params())


class CumulativeCommitCountResolverMixin(SelectablePropertyResolverMixin):

    cumulative_commit_count = graphene.Field(graphene.List(CumulativeCommitCountProperty))

    def resolve_cumulative_commit_count(self, info, **kwargs):
        return self.resolve_property('cumulative_commit_count')

