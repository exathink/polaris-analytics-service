# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar



import graphene
from sqlalchemy import select, bindparam, func
from polaris.analytics.db.model import teams
from polaris.graphql.interfaces import KeyIdNode, NamedNode



class TeamNode:
    interfaces = (NamedNode, )

    @staticmethod
    def selectable(**kwargs):
        return select([
            teams.c.id,
            teams.c.name,
            teams.c.key,
        ]).where(
            teams.c.key == bindparam('key')
        )


