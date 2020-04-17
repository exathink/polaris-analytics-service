"""update_table_work_item_delivery_cycles_add_cycle_time_column

Revision ID: 9e049da8b739
Revises: e92fe79eb27a
Create Date: 2020-04-17 12:47:38.742605

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9e049da8b739'
down_revision = 'e92fe79eb27a'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_item_delivery_cycles', sa.Column('cycle_time', sa.Integer(), nullable=True), schema='analytics')


def downgrade():
    op.drop_column('work_item_delivery_cycles', 'cycle_time', schema='analytics')
