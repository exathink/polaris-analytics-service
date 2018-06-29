# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import sys

from polaris.utils.config import get_config_provider
from polaris.flask.common import PolarisSecuredService

from polaris.analytics.service.viz_api import viz_api
from polaris.analytics.service.activity_summary_api import activity_summary_api
from polaris.analytics.service.activity_level_api import activity_level_api


class PolarisAnalyticsService(PolarisSecuredService):
    def __init__(self, import_name, db_url, db_connect_timeout=30, models=None,
                 public_paths=None, **kwargs):
        super(PolarisAnalyticsService, self).__init__(
            import_name, db_url, db_connect_timeout,
            models=models,
            public_paths=public_paths,
            **kwargs
        )
        self.public_paths.extend([])


config_provider = get_config_provider()
app = PolarisAnalyticsService(
    __name__,
    db_url=config_provider.get('POLARIS_DB_URL')
)


# Register endpoints
app.register_blueprint(viz_api, url_prefix='/data')
app.register_blueprint(activity_summary_api, url_prefix='/data/activity-summary')
app.register_blueprint(activity_level_api, url_prefix='/data/activity-level')


# for dev mode use only.
if __name__ == "__main__":
    # Pycharm optimized settings.
    # Debug is turned off by default (use PyCharm debugger)
    # reloader is turned on by default so that we can get hot code reloading
    DEBUG = '--debug' in sys.argv
    RELOAD = '--no-reload' not in sys.argv
    app.run(host='0.0.0.0', port=8200, debug=DEBUG, use_reloader=RELOAD)