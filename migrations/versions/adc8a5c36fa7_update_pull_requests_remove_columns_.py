"""update_pull_requests_remove_columns_related_to_branches_table

Revision ID: adc8a5c36fa7
Revises: ad7dee0d068d
Create Date: 2020-07-06 11:57:26.627215

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'adc8a5c36fa7'
down_revision = 'ad7dee0d068d'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column('pull_requests', 'source_branch_latest_commit', schema='analytics')


def downgrade():
    op.add_column('pull_requests', sa.Column('source_branch_latest_commit', sa.VARCHAR(), autoincrement=False, nullable=True), schema='analytics')
