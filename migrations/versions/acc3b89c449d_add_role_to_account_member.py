"""add_role_to_account_member

Revision ID: acc3b89c449d
Revises: 516463d87d7d
Create Date: 2019-05-26 17:59:30.302418

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'acc3b89c449d'
down_revision = '516463d87d7d'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('account_members', sa.Column('role', sa.String(), server_default='member', nullable=False), schema='analytics')


def downgrade():
    op.drop_column('account_members', 'role', schema='analytics')

