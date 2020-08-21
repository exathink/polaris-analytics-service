"""add_effort_column_to_work_items_n_delivery_cycles

Revision ID: e940baf6688e
Revises: 69c93b8aa4e7
Create Date: 2020-08-21 20:38:26.939853

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'e940baf6688e'
down_revision = '69c93b8aa4e7'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_item_delivery_cycles', sa.Column('effort', sa.Float(), nullable=True), schema='analytics')
    op.add_column('work_items', sa.Column('effort', sa.Float(), nullable=True), schema='analytics')



def downgrade():
    op.drop_column('work_items', 'effort', schema='analytics')
    op.drop_column('work_item_delivery_cycles', 'effort', schema='analytics')

