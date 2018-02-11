# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from flask import Blueprint
from flask_cors import cross_origin

chart_api = Blueprint('chart_api', __name__)

@chart_api.route('/')
def index():
    return 'ping'

