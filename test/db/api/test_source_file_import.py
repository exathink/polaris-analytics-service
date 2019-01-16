# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from test.constants import *
from test.fixtures.commit_details import *
from polaris.analytics.db import api

class TestSourceFileImport:

    def it_registers_source_files(self, import_commit_details_fixture):
        keys = import_commit_details_fixture
        payload = dict(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            commit_details=[
                dict(
                    source_commit_id='1000',
                    key=keys[0],
                    parents=['99', '100'],
                    stats=dict(
                        files=1,
                        lines=10,
                        insertions=8,
                        deletions=2
                    ),
                    source_files=[
                        dict(
                            key=uuid.uuid4().hex,
                            path='test/',
                            name='files1.txt',
                            file_type='txt',
                            version_count=1,
                            is_deleted=False,
                            action='A',
                            stats={"lines": 2, "insertions": 2, "deletions": 0}
                        ),
                        dict(
                            key=uuid.uuid4().hex,
                            path='test/',
                            name='files2.py',
                            file_type='py',
                            version_count=1,
                            is_deleted=False,
                            action='A',
                            stats={"lines": 2, "insertions": 2, "deletions": 0}
                        )
                    ]
                )
            ]
        )
        result = api.register_source_file_versions(**payload)
        assert result['success']
        assert db.connection().execute(
            "select count(id) from analytics.source_files").scalar() == 2

    def it_allows_commits_with_empty_source_file_lists(self, import_commit_details_fixture):
        keys = import_commit_details_fixture
        payload = dict(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            commit_details=[
                dict(
                    source_commit_id='1000',
                    key=keys[0],
                    parents=['99', '100'],
                    stats=dict(
                        files=1,
                        lines=10,
                        insertions=8,
                        deletions=2
                    ),
                    source_files=[]
                )
            ]
        )
        result = api.register_source_file_versions(**payload)
        assert result['success']
        assert db.connection().execute(
            "select count(id) from analytics.source_files").scalar() == 0

    def it_inserts_source_files_correctly_with_multiple_commits(self, import_commit_details_fixture):
        keys = import_commit_details_fixture
        payload = dict(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            commit_details=[
                dict(
                    source_commit_id=f"{key}",
                    key=keys[1000 - key].hex,
                    parents=['99', '100'],
                    stats=dict(
                        files=1,
                        lines=10,
                        insertions=8,
                        deletions=2
                    ),
                    source_files=[
                        dict(
                            key=keys[0].hex,
                            path='test/',
                            name='files1.txt',
                            file_type='txt',
                            version_count=1000 - key + 1,
                            is_deleted=False,
                            action='U',
                            stats={"lines": 2, "insertions": 2, "deletions": 0}
                        ),
                        dict(
                            key=uuid.uuid4().hex,
                            path='test/',
                            name=f'files{key}.py',
                            file_type='py',
                            version_count=1,
                            is_deleted=False,
                            action='A',
                            stats={"lines": 2, "insertions": 2, "deletions": 0}
                        )
                    ]
                )
                for key in range(1000, 1010)
            ]
        )
        result = api.register_source_file_versions(**payload)
        assert result['success']
        assert db.connection().execute("select count(id) from analytics.source_files").scalar() == 11

    def it_correctly_sets_the_version_number_of_files_when_the_same_file_is_in_multiple_commits(self,
                                                                                                import_commit_details_fixture):
        commit_keys = import_commit_details_fixture
        file1_key = uuid.uuid4()

        payload = dict(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            commit_details=[
                # First commit
                dict(
                    source_commit_id="1000",
                    key=commit_keys[0].hex,
                    parents=['99', '100'],
                    stats=dict(
                        files=1,
                        lines=10,
                        insertions=8,
                        deletions=2
                    ),
                    source_files=[
                        dict(
                            key=file1_key.hex,
                            path='test/',
                            name='files1.txt',
                            file_type='txt',
                            version_count=2,
                            is_deleted=False,
                            action='U',
                            stats={"lines": 2, "insertions": 2, "deletions": 0}
                        ),
                        dict(
                            key=uuid.uuid4().hex,
                            path='test/',
                            name='files2.txt',
                            file_type='txt',
                            version_count=1,
                            is_deleted=False,
                            action='U',
                            stats={"lines": 2, "insertions": 2, "deletions": 0}
                        )

                    ]
                ),
                # second commit: has new version of file1
                dict(
                    source_commit_id="1000",
                    key=commit_keys[0].hex,
                    parents=['99', '100'],
                    stats=dict(
                        files=1,
                        lines=10,
                        insertions=8,
                        deletions=2
                    ),
                    source_files=[
                        dict(
                            key=file1_key.hex,
                            path='test/',
                            name='files1.txt',
                            file_type='txt',
                            version_count=1,
                            is_deleted=False,
                            action='U',
                            stats={"lines": 2, "insertions": 2, "deletions": 0}
                        ),
                        dict(
                            key=uuid.uuid4().hex,
                            path='test/',
                            name='files3.txt',
                            file_type='txt',
                            version_count=1,
                            is_deleted=False,
                            action='U',
                            stats={"lines": 2, "insertions": 2, "deletions": 0}
                        )

                    ]
                )

            ]
        )
        result = api.register_source_file_versions(**payload)
        assert result['success']
        assert db.connection().execute("select count(id) from analytics.source_files").scalar() == 3

        assert db.connection().execute(
            f"select version_count from analytics.source_files where key='{file1_key}'").scalar() == 2

    def it_ensures_that_the_version_count_of_existing_files_is_monotonically_non_decreasing(self,
                                                                                            import_commit_details_fixture):
        commit_keys = import_commit_details_fixture
        file1_key = uuid.uuid4()

        # insert an existing source file: the existing version count is greater than the version count of commits we will
        # add. It should be preserved.
        with db.orm_session() as session:
            repo = model.Repository.find_by_repository_key(session, rails_repository_key)
            repo.source_files.append(
                model.SourceFile(
                    key=file1_key.hex,
                    path='test/',
                    name='files1.txt',
                    file_type='txt',
                    version_count=3
                )
            )

        payload = dict(
            organization_key=rails_organization_key,
            repository_key=rails_repository_key,
            commit_details=[
                # First commit: version count = 2
                dict(
                    source_commit_id="1000",
                    key=commit_keys[0].hex,
                    parents=['99', '100'],
                    stats=dict(
                        files=1,
                        lines=10,
                        insertions=8,
                        deletions=2
                    ),
                    source_files=[
                        dict(
                            key=file1_key.hex,
                            path='test/',
                            name='files1.txt',
                            file_type='txt',
                            version_count=2,
                            is_deleted=False,
                            action='U',
                            stats={"lines": 2, "insertions": 2, "deletions": 0}
                        )

                    ]
                ),
                # second commit: has new version of file1: version count = 1
                dict(
                    source_commit_id="1000",
                    key=commit_keys[0].hex,
                    parents=['99', '100'],
                    stats=dict(
                        files=1,
                        lines=10,
                        insertions=8,
                        deletions=2
                    ),
                    source_files=[
                        dict(
                            key=file1_key.hex,
                            path='test/',
                            name='files1.txt',
                            file_type='txt',
                            version_count=1,
                            is_deleted=False,
                            action='U',
                            stats={"lines": 2, "insertions": 2, "deletions": 0}
                        )
                    ]
                )

            ]
        )
        result = api.import_commit_details(**payload)
        assert result['success']
        assert db.connection().execute("select count(id) from analytics.source_files").scalar() == 1

        assert db.connection().execute(
            f"select version_count from analytics.source_files where key='{file1_key}'").scalar() == 3