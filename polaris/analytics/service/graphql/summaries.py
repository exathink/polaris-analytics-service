# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import graphene


class ActivityLevelSummary(graphene.ObjectType):
    active_count = graphene.Int(required=False)
    quiescent_count = graphene.Int(required=False)
    dormant_count = graphene.Int(required=False)
    inactive_count = graphene.Int(required=False)


class Inceptions(graphene.ObjectType):
    year = graphene.Int(required=True)
    month = graphene.Int(required=False)
    week = graphene.Int(required=False)
    inceptions = graphene.Int(required=True)