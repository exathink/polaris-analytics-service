"""Added profile column to accounts

Revision ID: c149c9445482
Revises: 7b57db25b731
Create Date: 2019-05-14 20:11:07.280168

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'c149c9445482'
down_revision = '7b57db25b731'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('accounts', sa.Column('profile', postgresql.JSONB(astext_type=sa.Text()), server_default='{}', nullable=False), schema='analytics')


def downgrade():
    op.drop_column('accounts', 'profile', schema='analytics')
