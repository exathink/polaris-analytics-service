"""add_owner_key_to_account

Revision ID: 3bdbaa68e83e
Revises: dcd8cc4e9a46
Create Date: 2019-05-22 00:57:35.421813

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '3bdbaa68e83e'
down_revision = 'dcd8cc4e9a46'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('accounts', sa.Column('owner_key', postgresql.UUID(), nullable=True), schema='analytics')



def downgrade():
    op.drop_column('accounts', 'owner_key', schema='analytics')

