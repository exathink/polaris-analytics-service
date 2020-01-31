"""add_state_type_to_work_items

Revision ID: 981b74c1f9a4
Revises: b5889d59dc55
Create Date: 2020-01-31 19:15:07.272377

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '981b74c1f9a4'
down_revision = 'b5889d59dc55'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('work_items', sa.Column('state_type', sa.String(), nullable=True), schema='analytics')


def downgrade():
    op.drop_column('work_items', 'state_type', schema='analytics')
