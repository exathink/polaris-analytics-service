# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.graphql.interfaces import NamedNode
from polaris.analytics.db.model import feature_flags

from sqlalchemy import select, bindparam
#from ..interfaces import FeatureFlagInfo


class FeatureFlagNode:
    interfaces = (NamedNode,)

    @staticmethod
    def selectable(**kwargs):
        return select([
            feature_flags.c.id,
            feature_flags.c.key,
            feature_flags.c.name
        ]).where(
            feature_flags.c.key == bindparam('key')
        )