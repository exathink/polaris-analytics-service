# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2020) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from datetime import datetime, timedelta

from sqlalchemy import select, cast, func, Date

from polaris.utils.exceptions import ProcessingException


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


def get_before_date(**kwargs):
    before_date = kwargs.get('before')
    if before_date:
        if isinstance(before_date, datetime):
            return before_date.date() + timedelta(days=1)
        else:
            return before_date + timedelta(days=1)
    else:
        return datetime.utcnow()


def get_measurement_period(trends_args, arg_name=None, interface_name=None):
    if trends_args is None:
        raise ProcessingException(
            f"'{arg_name}' is a required arg to resolve the "
            f"{interface_name} interface"
        )

    # The end date of the measurement period.
    measurement_period_end_date = trends_args.before or datetime.utcnow()

    # This parameter specified the window of time for which we are reporting
    # the trends - so for example, average cycle time over the past 15 days
    # => days = 15. We will take a set of measurements over the
    # the 15 day period and report metrics for each measurement date.
    days = trends_args.days
    if days is None:
        raise ProcessingException(
            f"The argument 'days' must be specified when resolving the interface {interface_name}"
        )

    # the start date of the measurement period
    measurement_period_start_date = measurement_period_end_date - timedelta(
        days=days
    )

    return measurement_period_start_date, measurement_period_end_date


def get_timeline_dates_for_trending(trends_args, arg_name=None, interface_name=None):
    measurement_period_start_date, measurement_period_end_date = get_measurement_period(
        trends_args, arg_name, interface_name
    )
    # First we generate a series of dates between the period start and end dates
    # at the granularity of the sampling frequency parameter.
    return select([
        cast(
            func.generate_series(
                measurement_period_end_date,
                measurement_period_start_date,
                timedelta(days=-1 * trends_args.sampling_frequency)
            ),
            Date
        ).label('measurement_date')
    ]).alias()