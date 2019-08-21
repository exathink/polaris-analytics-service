"""make_commit_mapping_scope_key_nullable

Revision ID: c74cb2b8db51
Revises: 26d2096582bb
Create Date: 2019-08-20 21:17:39.140391

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'c74cb2b8db51'
down_revision = '26d2096582bb'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('work_items_sources', 'commit_mapping_scope_key',
               existing_type=postgresql.UUID(),
               nullable=True,
               schema='analytics')
    # ### end Alembic commands ###


def downgrade():
    op.alter_column('work_items_sources', 'commit_mapping_scope_key',
               existing_type=postgresql.UUID(),
               nullable=False,
               schema='analytics')
    # ### end Alembic commands ###
