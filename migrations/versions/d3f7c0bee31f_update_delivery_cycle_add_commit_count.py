"""update_delivery_cycle_add_commit_count

Revision ID: d3f7c0bee31f
Revises: 11795bf4a18b
Create Date: 2020-03-31 17:37:12.085968

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd3f7c0bee31f'
down_revision = '11795bf4a18b'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_item_delivery_cycles', sa.Column('commit_count', sa.Integer(), nullable=True), schema='analytics')


def downgrade():
    op.drop_column('work_item_delivery_cycles', 'commit_count', schema='analytics')
