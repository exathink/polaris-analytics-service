# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from datetime import datetime, timedelta
from sqlalchemy import select, cast, func, Date

from polaris.utils.exceptions import ProcessingException
from polaris.analytics.db.model import work_items, work_items_sources, work_item_delivery_cycles
from ..work_item.sql_expressions import apply_specs_only_filter, work_items_connection_apply_filters, work_item_delivery_cycles_connection_apply_filters


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


def select_non_closed_work_items(project_nodes, select_work_items_columns, **kwargs):
    non_closed_work_items = select(select_work_items_columns).select_from(
        project_nodes.join(
            work_items_sources, work_items_sources.c.project_id == project_nodes.c.id,
        ).join(
            work_items, work_items.c.work_items_source_id == work_items_sources.c.id
        ).join(
            # here we only include the current delivery cycles.
            work_item_delivery_cycles,
            work_items.c.current_delivery_cycle_id == work_item_delivery_cycles.c.delivery_cycle_id
        )
    ).where(
        work_item_delivery_cycles.c.end_date == None
    )
    # apply work item filters
    non_closed_work_items = work_items_connection_apply_filters(
        non_closed_work_items,
        work_items,
        **kwargs
    )

    # apply the specs only filter for work_item_delivery_cycles. Note we cannot apply the
    # closed within days filter to open items since it will filter everything out.
    # that's why we are explicitly only including the specs_only _filter.
    non_closed_work_items = apply_specs_only_filter(
        non_closed_work_items,
        work_item_delivery_cycles,
        **kwargs
    )

    return non_closed_work_items


def select_closed_work_items(project_nodes, select_work_items_columns, **kwargs):
    closed_work_items = select(select_work_items_columns).select_from(
        project_nodes.join(
            work_items_sources, work_items_sources.c.project_id == project_nodes.c.id,
        ).join(
            work_items, work_items.c.work_items_source_id == work_items_sources.c.id
        ).join(
            # This includes all closed delivery cycles of a work item so that we match
            # the calculations/counts for the Closed items metrics.
            work_item_delivery_cycles,
            work_item_delivery_cycles.c.work_item_id == work_items.c.id
        )
    ).where(
        work_item_delivery_cycles.c.end_date != None
    )
    # Apply the standard filters for work items and work items delivery cycles here.
    # For closed items we apply the delivery cycle filters so that we match
    # the values that are calculated for closed item flow metrics.
    closed_work_items = work_item_delivery_cycles_connection_apply_filters(
        closed_work_items,
        work_items,
        work_item_delivery_cycles,
        **kwargs
    )
    return closed_work_items