"""add_latency_to_delivery_cycles

Revision ID: e7ae36eb0671
Revises: c8a96a2142aa
Create Date: 2020-09-26 21:35:28.782443

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e7ae36eb0671'
down_revision = 'c8a96a2142aa'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_item_delivery_cycles', sa.Column('latency', sa.Integer(), nullable=True), schema='analytics')
    op.execute("update analytics.work_item_delivery_cycles "
               "set latency = extract('epoch' from end_date - coalesce(latest_commit, end_date))")


def downgrade():
    op.drop_column('work_item_delivery_cycles', 'latency', schema='analytics')
