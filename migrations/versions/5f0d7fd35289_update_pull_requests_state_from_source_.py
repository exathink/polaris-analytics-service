"""update_pull_requests_state_from_source_state.py

Revision ID: 5f0d7fd35289
Revises: aad95422f1c1
Create Date: 2020-08-05 07:58:11.850194

"""
from alembic import op
import sqlalchemy as sa
from polaris.common.enums import GithubPullRequestState, GitlabPullRequestState, VcsIntegrationTypes

# revision identifiers, used by Alembic.
revision = '5f0d7fd35289'
down_revision = 'aad95422f1c1'
branch_labels = None
depends_on = None


def update_pull_request_state():
    # gitlab
    op.execute(f"""
                update analytics.pull_requests set state='{GitlabPullRequestState.open.value}' 
                from analytics.repositories 
                where source_state='open' 
                and analytics.repositories.id = analytics.pull_requests.repository_id 
                and analytics.repositories.integration_type='{VcsIntegrationTypes.gitlab.value}'
            """)
    op.execute(f"""
                    update analytics.pull_requests set state='{GitlabPullRequestState.merged.value}' 
                    from analytics.repositories 
                    where source_state='merged' 
                    and analytics.repositories.id = analytics.pull_requests.repository_id 
                    and analytics.repositories.integration_type='{VcsIntegrationTypes.gitlab.value}'
                """)
    op.execute(f"""
                    update analytics.pull_requests set state='{GitlabPullRequestState.closed.value}' 
                    from analytics.repositories 
                    where source_state='closed' 
                    and analytics.repositories.id = analytics.pull_requests.repository_id 
                    and analytics.repositories.integration_type='{VcsIntegrationTypes.gitlab.value}'
                """)
    op.execute(f"""
                        update analytics.pull_requests set state='{GitlabPullRequestState.locked.value}' 
                        from analytics.repositories 
                        where source_state='locked' 
                        and analytics.repositories.id = analytics.pull_requests.repository_id 
                        and analytics.repositories.integration_type='{VcsIntegrationTypes.gitlab.value}'
                    """)
    # github
    op.execute(f"""
                    update analytics.pull_requests set state='{GithubPullRequestState.open.value}' 
                    from analytics.repositories 
                    where source_state='open' 
                    and analytics.repositories.id = analytics.pull_requests.repository_id 
                    and analytics.repositories.integration_type='{VcsIntegrationTypes.github.value}'
                """)
    op.execute(f"""
                        update analytics.pull_requests set state='{GithubPullRequestState.merged.value}' 
                        from analytics.repositories 
                        where source_state='merged' 
                        and analytics.repositories.id = analytics.pull_requests.repository_id 
                        and analytics.repositories.integration_type='{VcsIntegrationTypes.github.value}'
                    """)
    op.execute(f"""
                        update analytics.pull_requests set state='{GithubPullRequestState.closed.value}' 
                        from analytics.repositories 
                        where source_state='closed' 
                        and analytics.repositories.id = analytics.pull_requests.repository_id 
                        and analytics.repositories.integration_type='{VcsIntegrationTypes.github.value}'
                    """)


def upgrade():
    update_pull_request_state()


def downgrade():
    op.execute(f"""
                update analytics.pull_requests set state=NULL
            """)
