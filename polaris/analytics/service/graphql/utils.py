# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2020) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from datetime import datetime, timedelta


def parse_json_timestamp(timestamp):
    if timestamp is not None:
        try:
            return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")


# Utility function to encapsulate the windowing logic
def date_column_is_in_measurement_window(date_column, measurement_date, measurement_window):
    # for the given measurement date which may be a timestamp, set the measurement_date to the date
    # value for that timestamp. This normalizes measurements to begin at the day boundary.
    # we add the instance check here to allow us to use this with measurement_dates that are columns
    # in  a table as well.
    if isinstance(measurement_date, datetime):
        measurement_date = measurement_date.date()
    # we will end the window at the end of the day of the day of measurement, so we always include the
    # items that occur on the measurement day in the filter.
    window_end = measurement_date + timedelta(days=1)
    # since we include the measurement day in the window, we reduce the overall measurements
    # days by 1 day in starting the window - so we go measurement_window - 1 days back from the measurement_date
    window_start = measurement_date - timedelta(days=measurement_window - 1)

    return date_column.between(window_start, window_end)


def compare_with_before_date(date_column, before):
    if before:
        before_date = before.date() + timedelta(days=1)
    else:
        before_date = datetime.utcnow()
    return date_column.lt(before_date)
