# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.graphql.mixins import *

from .interfaces import StateTypeAggregateMeasure


class ContributorCountResolverMixin(KeyIdResolverMixin):
    def __init__(self, *args, **kwargs):
        self.contributor_count = None
        super().__init__(*args, **kwargs)

    def resolve_contributor_count(self, info, **kwargs):
        return 0 if self.contributor_count is None else self.contributor_count


class WorkItemStateTypeSummaryResolverMixin(KeyIdResolverMixin):
    def __init__(self, *args, **kwargs):
        self.work_item_state_type_counts = []
        super().__init__(*args, **kwargs)

    def resolve_work_item_state_type_counts(self, info, **kwargs):
        return StateTypeAggregateMeasure(**{
            result['state_type']: result['count']
            for result in self.work_item_state_type_counts
        })
