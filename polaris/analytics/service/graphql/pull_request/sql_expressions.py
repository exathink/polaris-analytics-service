# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from sqlalchemy import func, case, and_
from datetime import datetime, timedelta
from polaris.analytics.db.model import pull_requests
from polaris.analytics.service.graphql.utils import date_column_is_in_measurement_window

def pull_request_info_columns(pull_requests):
    return [
        pull_requests.c.id,
        pull_requests.c.key,
        pull_requests.c.title.label('name'),
        pull_requests.c.display_id,
        pull_requests.c.web_url.label('web_url'),
        pull_requests.c.created_at,
        pull_requests.c.state,
        pull_requests.c.end_date,
        (func.extract('epoch',
                      case(
                          [
                              (pull_requests.c.state != 'open',
                               (pull_requests.c.updated_at - pull_requests.c.created_at))
                          ],
                          else_=(datetime.utcnow() - pull_requests.c.created_at)
                      )

                      ) / (1.0 * 3600 * 24)).label('age')
    ]


def pull_requests_connection_apply_filters(select_pull_requests, **kwargs):
    if kwargs.get('active_only'):
        select_pull_requests = select_pull_requests.where(pull_requests.c.state == 'open')
    if 'closed_within_days' in kwargs and kwargs['closed_within_days'] > 0:
        before_date = kwargs.get('before', datetime.utcnow())

        select_pull_requests = select_pull_requests.where(
            and_(
                pull_requests.c.end_date != None,
                date_column_is_in_measurement_window(
                    pull_requests.c.end_date,
                    measurement_date=before_date,
                    measurement_window=kwargs.get('closed_within_days')
                )
            )
        )
    return select_pull_requests
