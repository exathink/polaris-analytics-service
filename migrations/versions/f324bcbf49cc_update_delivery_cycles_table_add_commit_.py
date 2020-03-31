"""update_delivery_cycles_table_add_commit_span_columns

Revision ID: f324bcbf49cc
Revises: af1817e282a8
Create Date: 2020-03-27 16:12:17.843275

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f324bcbf49cc'
down_revision = 'af1817e282a8'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_item_delivery_cycles', sa.Column('earliest_commit', sa.DateTime(), nullable=True), schema='analytics')
    op.add_column('work_item_delivery_cycles', sa.Column('latest_commit', sa.DateTime(), nullable=True), schema='analytics')


def downgrade():
    op.drop_column('work_item_delivery_cycles', 'latest_commit', schema='analytics')
    op.drop_column('work_item_delivery_cycles', 'earliest_commit', schema='analytics')
