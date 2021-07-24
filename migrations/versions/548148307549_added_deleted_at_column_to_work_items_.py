"""added_deleted_at_column_to_work_items_table

Revision ID: 548148307549
Revises: 194fb3b1ab35
Create Date: 2021-07-09 09:28:01.095266

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '548148307549'
down_revision = '194fb3b1ab35'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_items', sa.Column('deleted_at', sa.DateTime(), nullable=True), schema='analytics')


def downgrade():
    op.drop_column('work_items', 'deleted_at', schema='analytics')
