"""add_organization_member_table

Revision ID: a636d61c1e41
Revises: acc3b89c449d
Create Date: 2019-05-27 13:09:51.731614

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'a636d61c1e41'
down_revision = 'acc3b89c449d'
branch_labels = None
depends_on = None


def upgrade():

    op.create_table('organization_members',
    sa.Column('organization_id', sa.Integer(), nullable=False),
    sa.Column('user_key', postgresql.UUID(as_uuid=True), nullable=False),
    sa.Column('role', sa.String(), server_default='member', nullable=False),
    sa.ForeignKeyConstraint(['organization_id'], ['analytics.organizations.id'], ),
    sa.PrimaryKeyConstraint('organization_id', 'user_key'),
    schema='analytics'
    )


def downgrade():
    op.drop_table('organization_members', schema='analytics')
