"""add_effort_to_delivery_cycles

Revision ID: 64452fc7e7f8
Revises: e8e27c64c692
Create Date: 2020-08-25 01:06:12.802344

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '64452fc7e7f8'
down_revision = 'e8e27c64c692'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_item_delivery_cycles', sa.Column('effort', sa.Float(), nullable=True), schema='analytics')


def downgrade():
    op.drop_column('work_item_delivery_cycles', 'effort', schema='analytics')
