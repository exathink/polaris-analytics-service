# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from datetime import datetime, timedelta
from sqlalchemy import select, cast, func, Date

from polaris.utils.exceptions import ProcessingException


def get_timeline_dates_for_trending(trends_args, arg_name=None, interface_name=None):

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

    # This metric specifies the window over which values are aggregated for *each* point in the measurement window.
    # So for example if we are reporting the 30 day average cycle time over the past 15 days,
    # days = 15 and cycle_metric_trends_measurement_window = 30. For each
    # measurement date in the 15 day measurement period, we will find the average cycle time of
    # the work items that have closed in the previous 30 days. The idea is to replicate the trend values
    # that we are seeing in the snapshot Flow Metrics view over time.
    measurement_window = trends_args.measurement_window
    if measurement_window is None:
        raise ProcessingException(
            f"The argument 'measurementWindow' must be specified when resolving the interface {interface_name}"
        )

    # the start date of the measurement period
    measurement_period_start_date = measurement_period_end_date - timedelta(
        days=days
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
