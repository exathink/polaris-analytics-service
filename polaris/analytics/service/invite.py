# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import logging
from urllib import parse

from flask_security.utils import send_mail

from polaris.utils.config import get_config_provider

config_provider = get_config_provider()

log = logging.getLogger('polaris.analytics.service.invite')


def send_reset_password_instructions(user, invitation):
    """Sends the reset password instructions email for the specified user.

    :param user: The user to send the instructions to
    """
    logging.info(f"Sending invite to {user.email}")
    auth_service_url = config_provider.get('AUTH_SERVICE_URL')
    if auth_service_url:
        p = parse.urlsplit(auth_service_url)
        reset_link = parse.urlunsplit((
            p.scheme,
            p.netloc,
            f'{p.path}/reset',
            p.query,
            p.fragment
        ))
        send_mail(invitation.get('subject', 'Welcome to Urjuna'), user.email,
                  'invite_owner',
                  user=user, reset_link=reset_link)

        logging.info(f"Invite sent to {user.email}")
