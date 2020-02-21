# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Maneet Sehgal

import graphene

from polaris.graphql.selectable import Selectable


class FeatureFlagEnablement(
    Selectable
):
    class Meta:
        interfaces = ()
        interface_resolvers = {}

    @classmethod
    def Field(cls, **kwargs):
        return super().Field(key_is_required=False, **kwargs)
