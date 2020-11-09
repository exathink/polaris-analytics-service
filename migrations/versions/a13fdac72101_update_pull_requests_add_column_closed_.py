"""update_pull_requests_add_column_closed_at

Revision ID: a13fdac72101
Revises: 152e8b6f0497
Create Date: 2020-11-03 14:37:10.135782

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a13fdac72101'
down_revision = '152e8b6f0497'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('pull_requests', sa.Column('end_date', sa.DateTime(), nullable=True), schema='analytics')
    op.execute("update analytics.pull_requests set end_date=(CASE WHEN state='merged' THEN merged_at WHEN state='closed' THEN updated_at END)")
    op.drop_column('pull_requests', 'merged_at', schema='analytics')


def downgrade():
    op.add_column('pull_requests', sa.Column('merged_at', sa.DateTime(), nullable=True), schema='analytics')
    op.execute('update analytics.pull_requests set merged_at=end_date')
    op.drop_column('pull_requests', 'end_date', schema='analytics')
