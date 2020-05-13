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
    def resolve(cls, commit_message, branch_name):
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
            else:
                raise ProcessingException(
                    f'WorkItemResolver: Could not find work item resolver for integration_type: {integration_type}')
        else:
            raise ProcessingException(f'WorkItemResolver: Cannot resolve null integration type')


class PivotalTrackerWorkItemResolver(WorkItemResolver):
    stories = re.compile("[#'](\d+)")
    branch = re.compile('^#?(\d+)$')  # the hash is optional for matching in branch names

    @classmethod
    def resolve(cls, commit_message, branch_name):
        resolved = []
        # check commit message for matches
        resolved.extend(cls.stories.findall(commit_message))
        # check branch name for matches
        if branch_name is not None:
            resolved.extend(cls.branch.findall(branch_name))
        return resolved


class GithubWorkItemResolver(WorkItemResolver):
    commit_message_matcher = re.compile('#(\d+)')
    branch = re.compile('^#?(\d+)$')  # the hash is optional for matching in branch names

    @classmethod
    def resolve(cls, commit_message, branch_name):
        resolved = []
        resolved.extend(cls.commit_message_matcher.findall(commit_message))

        if branch_name is not None:
            resolved.extend(cls.branch.findall(branch_name))

        return resolved


class JiraWorkItemResolver(WorkItemResolver):
    matcher = re.compile('([\S]+-\d+)')

    @classmethod
    def resolve(cls, commit_message, branch_name):
        resolved = []
        resolved.extend(cls.matcher.findall(commit_message))
        if branch_name is not None:
            resolved.extend(cls.matcher.findall(branch_name))

        return resolved
