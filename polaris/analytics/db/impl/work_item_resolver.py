# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import re
from polaris.utils.exceptions import ProcessingException
from polaris.common.enums import WorkTrackingIntegrationType


class WorkItemResolver:

    @classmethod
    def resolve(cls, *text_tokens, branch_name=None):
        raise NotImplementedError

    @classmethod
    def get_resolver(cls, integration_type):
        if integration_type is not None:
            if integration_type in ['github', 'github_enterprise']:
                return GithubWorkItemResolver
            elif integration_type in ['pivotal_tracker']:
                return PivotalTrackerWorkItemResolver
            elif integration_type in ['jira']:
                return JiraWorkItemResolver
            elif integration_type in ['gitlab']:
                return GitlabWorkItemResolver
            else:
                raise ProcessingException(
                    f'WorkItemResolver: Could not find work item resolver for integration_type: {integration_type}')
        else:
            raise ProcessingException(f'WorkItemResolver: Cannot resolve null integration type')


class PivotalTrackerWorkItemResolver(WorkItemResolver):
    stories = re.compile("[#'](\d+)")
    branch = re.compile('^#?(\d+)$')  # the hash is optional for matching in branch names

    @classmethod
    def resolve(cls, *text_tokens, display_id=None, branch_name=None):
        resolved = []
        # check commit message for matches
        for text_token in text_tokens:
            resolved.extend(cls.stories.findall(text_token))
        # check branch name for matches
        if branch_name is not None:
            resolved.extend(cls.branch.findall(branch_name))
        return resolved


class GithubWorkItemResolver(WorkItemResolver):
    commit_message_matcher = re.compile('#(\d+)')
    branch_or_display_id = re.compile('^#?(\d+)$')  # the hash is optional for matching in branch names or display ids

    @classmethod
    def resolve(cls, *text_tokens, display_id=None, branch_name=None):
        resolved = []
        for text_token in text_tokens:
            resolved.extend(cls.commit_message_matcher.findall(text_token))

        if branch_name is not None:
            resolved.extend(cls.branch_or_display_id.findall(branch_name))

        # NOTE: Display ids for github match corresponding github work items. \
        # So this parameter is used only for Github.
        if display_id is not None:
            resolved.extend(cls.branch_or_display_id.findall(display_id))

        return resolved


class GitlabWorkItemResolver(WorkItemResolver):
    commit_message_matcher = re.compile('#(\d+)')
    branch_or_display_id = re.compile('^#?(\d+)$')  # the hash is optional for matching in branch names or display ids

    @classmethod
    def resolve(cls, *text_tokens, display_id=None, branch_name=None):
        resolved = []
        for text_token in text_tokens:
            resolved.extend(cls.commit_message_matcher.findall(text_token))

        if branch_name is not None:
            resolved.extend(cls.branch_or_display_id.findall(branch_name))

        return resolved


class JiraWorkItemResolver(WorkItemResolver):
    matcher = re.compile('([\w]+-\d+)')

    @classmethod
    def resolve(cls, *text_tokens, display_id=None, branch_name=None):
        resolved = []
        for text_token in text_tokens:
            resolved.extend(cls.matcher.findall(text_token))
        if branch_name is not None:
            resolved.extend(cls.matcher.findall(branch_name))

        return resolved
