# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


from flask import Blueprint, make_response
from flask_cors import cross_origin

viz_api = Blueprint('viz_api', __name__)

@viz_api.route('/')
@cross_origin()
def index():
    return 'ping'

