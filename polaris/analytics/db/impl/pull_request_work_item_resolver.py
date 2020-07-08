# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Pragya Goyal

import re
from polaris.utils.exceptions import ProcessingException


class PullRequestWorkItemResolver:

    @classmethod
    def resolve(cls, title, description, source_branch):
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


class PivotalTrackerWorkItemResolver(PullRequestWorkItemResolver):
    stories = re.compile("[#'](\d+)")
    branch = re.compile('^#?(\d+)$')  # the hash is optional for matching in branch names

    @classmethod
    def resolve(cls, title, description, source_branch):
        resolved = []
        # check commit message for matches
        resolved.extend(cls.stories.findall(title))
        resolved.extend(cls.stories.findall(description))
        # check branch name for matches
        if source_branch is not None:
            resolved.extend(cls.branch.findall(source_branch))
        return resolved


class GithubWorkItemResolver(PullRequestWorkItemResolver):
    pr_title_description_matcher = re.compile('#(\d+)')
    branch = re.compile('^#?(\d+)$')  # the hash is optional for matching in branch names

    @classmethod
    def resolve(cls, title, description, source_branch):
        resolved = []
        resolved.extend(cls.pr_title_description_matcher.findall(title))
        resolved.extend(cls.pr_title_description_matcher.findall(description))

        if source_branch is not None:
            resolved.extend(cls.branch.findall(source_branch))

        return resolved


class JiraWorkItemResolver(PullRequestWorkItemResolver):
    matcher = re.compile('([\w]+-\d+)')

    @classmethod
    def resolve(cls, title, description, source_branch):
        resolved = []
        resolved.extend(cls.matcher.findall(title))
        resolved.extend(cls.matcher.findall(description))
        if source_branch is not None:
            resolved.extend(cls.matcher.findall(source_branch))

        return resolved


