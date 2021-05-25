"""add_spec_cycle_time_to_delivery_cycles

Revision ID: fd63c9f125b3
Revises: b5693ac165c4
Create Date: 2021-05-23 13:39:41.917382

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'fd63c9f125b3'
down_revision = 'b5693ac165c4'
branch_labels = None
depends_on = None


def upgrade():

    op.add_column('work_item_delivery_cycles', sa.Column('spec_cycle_time', sa.Integer(), nullable=True), schema='analytics')
    op.execute("update analytics.work_item_delivery_cycles "
               "set spec_cycle_time = greatest(cycle_time, extract(epoch from end_date - earliest_commit)::integer)")


def downgrade():
    op.drop_column('work_item_delivery_cycles', 'spec_cycle_time', schema='analytics')