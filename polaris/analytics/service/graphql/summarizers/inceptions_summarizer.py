# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from polaris.graphql.connection_utils import ConnectionSummarizer
from polaris.graphql.exceptions import InvalidSummarizerException

from polaris.analytics.service.graphql.summaries import Inceptions
from collections import namedtuple
from polaris.graphql.utils import  create_tuple


inception_key = namedtuple('inception_key', 'year month week')
inceptions_tuple = create_tuple(Inceptions)

class InceptionsSummarizer(ConnectionSummarizer):
    class Meta:
        interface = Inceptions

    @classmethod
    def summarize_result_set(cls, result_set):
        inceptions = dict()
        for row in result_set:
            if hasattr(row, 'earliest_commit'):
                earliest_commit = row['earliest_commit']
                if earliest_commit:
                    inceptions_key = inception_key(
                        year=earliest_commit.year,
                        month=earliest_commit.month,
                        week=earliest_commit.isocalendar()[2]
                    )
                    inceptions[inceptions_key] = inceptions.setdefault(inceptions_key, 0) + 1
            else:
                raise InvalidSummarizerException(f"InceptionsSummarizer: Cannot summarize result set. "
                                                 f"Column 'earliest_commit' is missing")

        return [
            inceptions_tuple(
                year=inceptions_key.year,
                month=inceptions_key.month,
                week=inceptions_key.week,
                inceptions=count
            )
            for inceptions_key, count in inceptions.items()
        ]
