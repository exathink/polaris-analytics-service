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


def get_link(url_key, path=None):
    url = config_provider.get(url_key)
    if url:
        p = parse.urlsplit(url)
        return parse.urlunsplit((
            p.scheme,
            p.netloc,
            f'{p.path}/{path}' if path is not None else p.path,
            p.query,
            p.fragment
        ))


def send_new_member_invite(user, invitation):
    """Invites a new user to join an account and organization.
    This user will also need to reset their password, so the email contains a reset password link

    :param user: The user to send the instructions to
    """
    logging.info(f"Sending invite to {user.email}")
    send_mail(
        invitation.get('subject', 'Welcome to Urjuna'),
        user.email,
        'invite_member',
        user=user,
        get_started_link=get_link('AUTH_SERVICE_URL', 'reset'),
        signin_link=get_link('WEB_APP_URL')
    )
    logging.info(f"Invite sent to {user.email}")
    return True


def send_join_account_notice(user, invitation):
    """Send an existing user an invite to join another existing account other than their home
    account. In this case the password reset

    :param user: The user to send the instructions to
    """
    logging.info(f"Sending invite to {user.email}")
    send_mail(
        invitation.get('subject', 'Welcome to Urjuna'),
        user.email,
        'invite_member',
        user=user,
        get_started_link=get_link('WEB_APP_URL'),
        signin_link=get_link('WEB_APP_URL')
    )

    logging.info(f"Invite sent to {user.email}")
    return True
