# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

import re

class WorkItemResolver:

    @classmethod
    def resolve(cls, commit_message):
        raise NotImplementedError

    @classmethod
    def get_resolver(cls, integration_type):
        if integration_type in ['github', 'github_enterprise']:
            return GithubWorkItemResolver
        elif integration_type in ['pivotal_tracker']:
            return PivotalTrackerWorkItemResolver
        elif integration_type in ['jira']:
            return JiraWorkItemResolver


class PivotalTrackerWorkItemResolver(WorkItemResolver):
    brackets = re.compile(r'\[(.*)\]', re.DOTALL)
    stories = re.compile('#(\d+)')

    @classmethod
    def resolve(cls, commit_message):
        resolved = []
        groups = cls.brackets.findall(commit_message)
        for group in groups:
            resolved.extend(cls.stories.findall(group))
        return resolved


class GithubWorkItemResolver(WorkItemResolver):
    matcher = re.compile('#(\d+)')

    @classmethod
    def resolve(cls,commit_message):
        return cls.matcher.findall(commit_message)


class JiraWorkItemResolver(WorkItemResolver):
    matcher = re.compile('([A-Z]+-\d+)')

    @classmethod
    def resolve(cls,commit_message):
        return cls.matcher.findall(commit_message)