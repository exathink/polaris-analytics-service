"""add_effort_column_to_delivery_cycles

Revision ID: 085b5fe8c713
Revises: 69c93b8aa4e7
Create Date: 2020-08-21 15:36:55.459571

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '085b5fe8c713'
down_revision = '69c93b8aa4e7'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_item_delivery_cycles', sa.Column('effort', sa.Float(), nullable=True), schema='analytics')



def downgrade():
    op.drop_column('work_item_delivery_cycles', 'effort', schema='analytics')

