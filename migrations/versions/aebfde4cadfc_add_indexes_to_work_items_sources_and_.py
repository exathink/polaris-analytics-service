"""Add indexes to work_items_sources and work_items

Revision ID: aebfde4cadfc
Revises: a88ad00047ad
Create Date: 2019-01-29 22:47:24.715970

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'aebfde4cadfc'
down_revision = 'a88ad00047ad'
branch_labels = None
depends_on = None


def upgrade():

    op.create_index('ix_analytics_work_items_work_items_source_id_display_id', 'work_items', ['work_items_source_id', 'display_id'], unique=False, schema='analytics')
    op.create_index('ix_analytics_work_items_sources_commit_mapping_scope', 'work_items_sources', ['organization_key', 'commit_mapping_scope', 'commit_mapping_scope_key'], unique=False, schema='analytics')
    # ### end Alembic commands ###


def downgrade():

    op.drop_index('ix_analytics_work_items_sources_commit_mapping_scope', table_name='work_items_sources', schema='analytics')
    op.drop_index('ix_analytics_work_items_work_items_source_id_display_id', table_name='work_items', schema='analytics')
    # ### end Alembic commands ###
