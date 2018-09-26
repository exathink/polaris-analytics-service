# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import graphene
from .interfaces import CumulativeCommitCount, WeeklyContributorCount, FileTypesSummary


class CumulativeCommitCountField(graphene.ObjectType):
    class Meta:
        interfaces = (CumulativeCommitCount,)


class WeeklyContributorCountsField(graphene.ObjectType):
    class Meta:
        interfaces = (CumulativeCommitCount, WeeklyContributorCount)


class FileTypesSummaryField(graphene.ObjectType):
    class Meta:
        interfaces = (FileTypesSummary,)


