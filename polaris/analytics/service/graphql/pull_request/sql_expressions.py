# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal


def pull_request_info_columns(pull_requests):
    return [
        pull_requests.c.id,
        pull_requests.c.key,
        pull_requests.c.title.label('name'),
        pull_requests.c.created_at,
        pull_requests.c.state,
        pull_requests.c.end_date
    ]