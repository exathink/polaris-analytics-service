"""added work_tracking entities

Revision ID: a88ad00047ad
Revises: d2b0e600f1f9
Create Date: 2019-01-23 22:29:21.705481

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'a88ad00047ad'
down_revision = 'd2b0e600f1f9'
branch_labels = None
depends_on = None


def upgrade():

    op.create_table('work_items_sources',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('key', postgresql.UUID(as_uuid=True), nullable=False),
    sa.Column('name', sa.String(), nullable=False),
    sa.Column('organization_key', postgresql.UUID(as_uuid=True), nullable=False),
    sa.Column('integration_type', sa.String(), nullable=False),
    sa.Column('commit_mapping_scope', sa.String(), server_default='organization', nullable=False),
    sa.Column('commit_mapping_scope_key', postgresql.UUID(as_uuid=True), nullable=False),
    sa.Column('organization_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['organization_id'], ['analytics.organizations.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('key'),
    schema='analytics'
    )

    op.create_table('work_items',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('key', postgresql.UUID(as_uuid=True), nullable=False),
    sa.Column('name', sa.String(length=256), nullable=False),
    sa.Column('display_id', sa.String(), nullable=False),
    sa.Column('description', sa.Text(), nullable=True),
    sa.Column('is_bug', sa.Boolean(), server_default='FALSE', nullable=False),
    sa.Column('tags', postgresql.ARRAY(sa.String()), server_default='{}', nullable=False),
    sa.Column('url', sa.String(), nullable=True),
    sa.Column('state', sa.String(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('updated_at', sa.DateTime(), nullable=True),
    sa.Column('work_items_source_id', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['work_items_source_id'], ['analytics.work_items_sources.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('key'),
    schema='analytics'
    )

    op.create_table('work_items_commits',
    sa.Column('work_item_id', sa.BigInteger(), nullable=False),
    sa.Column('commit_id', sa.BigInteger(), nullable=False),
    sa.ForeignKeyConstraint(['commit_id'], ['analytics.commits.id'], ),
    sa.ForeignKeyConstraint(['work_item_id'], ['analytics.work_items.id'], ),
    sa.PrimaryKeyConstraint('work_item_id', 'commit_id'),
    schema='analytics'
    )

    op.create_index(op.f('ix_analytics_work_items_commits_commit_id'), 'work_items_commits', ['commit_id'], unique=False, schema='analytics')
    op.create_index(op.f('ix_analytics_work_items_commits_work_item_id'), 'work_items_commits', ['work_item_id'], unique=False, schema='analytics')
    # ### end Alembic commands ###


def downgrade():

    op.drop_index(op.f('ix_analytics_work_items_commits_work_item_id'), table_name='work_items_commits', schema='analytics')
    op.drop_index(op.f('ix_analytics_work_items_commits_commit_id'), table_name='work_items_commits', schema='analytics')
    op.drop_table('work_items_commits', schema='analytics')
    op.drop_table('work_items', schema='analytics')
    op.drop_table('work_items_sources', schema='analytics')
    # ### end Alembic commands ###
