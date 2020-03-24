"""update_work_item_table_add_commit_span_columns

Revision ID: 95547be745a0
Revises: af1817e282a8
Create Date: 2020-03-24 10:52:58.703173

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '95547be745a0'
down_revision = 'af1817e282a8'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_items', sa.Column('earliest_commit_id', sa.Integer(), nullable=True), schema='analytics')
    op.add_column('work_items', sa.Column('latest_commit_id', sa.Integer(), nullable=True), schema='analytics')


def downgrade():
    op.drop_column('work_items', 'latest_commit_id', schema='analytics')
    op.drop_column('work_items', 'earliest_commit_id', schema='analytics')
