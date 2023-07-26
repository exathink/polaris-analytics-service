"""add_priority_column_to_work_items

Revision ID: 9f1c9164f5a8
Revises: 16c3ad41c116
Create Date: 2023-07-21 17:56:39.002483

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9f1c9164f5a8'
down_revision = '16c3ad41c116'
branch_labels = None
depends_on = None


def upgrade():

    op.add_column('work_items', sa.Column('priority', sa.String(), nullable=True), schema='analytics')



def downgrade():

    op.drop_column('work_items', 'priority', schema='analytics')

