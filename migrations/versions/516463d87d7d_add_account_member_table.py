"""add_account_member_table

Revision ID: 516463d87d7d
Revises: 3bdbaa68e83e
Create Date: 2019-05-26 14:20:53.375021

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '516463d87d7d'
down_revision = '3bdbaa68e83e'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('account_members',
    sa.Column('account_id', sa.Integer(), nullable=False),
    sa.Column('user_key', postgresql.UUID(as_uuid=True), nullable=False),
    sa.ForeignKeyConstraint(['account_id'], ['analytics.accounts.id'], ),
    sa.PrimaryKeyConstraint('account_id', 'user_key'),
    schema='analytics'
    )



def downgrade():
    op.drop_table('account_members', schema='analytics')

