"""update_delivery_cycles_table_add_commit_span_columns

Revision ID: 9f4979b2c813
Revises: af1817e282a8
Create Date: 2020-03-27 07:54:39.430623

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9f4979b2c813'
down_revision = 'af1817e282a8'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_item_delivery_cycles', sa.Column('earliest_commit_id', sa.Integer(), nullable=True), schema='analytics')
    op.add_column('work_item_delivery_cycles', sa.Column('latest_commit_id', sa.Integer(), nullable=True), schema='analytics')


def downgrade():
    op.drop_column('work_item_delivery_cycles', 'latest_commit_id', schema='analytics')
    op.drop_column('work_item_delivery_cycles', 'earliest_commit_id', schema='analytics')
