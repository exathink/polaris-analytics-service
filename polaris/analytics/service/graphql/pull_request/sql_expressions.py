# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

from sqlalchemy import func, case
from datetime import datetime


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