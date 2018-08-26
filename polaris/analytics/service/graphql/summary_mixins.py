# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

class ActivityLevelSummaryResolverMixin:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.activity_level_summary = None

    def resolve_activity_level_summary(self, info, **kwargs):
        return self.activity_level_summary


class InceptionsResolverMixin:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.inceptions = None

    def resolve_inceptions(self, info, **kwargs):
        return self.inceptions