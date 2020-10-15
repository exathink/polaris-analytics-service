# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2020) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from datetime import datetime


def parse_json_timestamp(timestamp):
    if timestamp is not None:
        try:
            return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
