""" Add Commit and Contributor fact tables

Revision ID: 17e112a50991
Revises: 
Create Date: 2018-12-15 20:06:50.231371

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '17e112a50991'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('contributors',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('key', postgresql.UUID(as_uuid=True), nullable=False),
    sa.Column('name', sa.String(), nullable=False),
    sa.Column('source', sa.String(), nullable=False),
    sa.Column('source_alias', sa.String(), nullable=False),
    sa.Column('alias_for', postgresql.UUID(as_uuid=True), nullable=True),
    sa.ForeignKeyConstraint(['alias_for'], ['analytics.contributors.key'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('key'),
    schema='analytics'
    )
    op.create_index('ix_analytics_contributors_key_alias_for', 'contributors', ['key', 'alias_for'], unique=False, schema='analytics')


    op.create_table('commits',
    sa.Column('id', sa.BigInteger(), nullable=False),
    sa.Column('key', postgresql.UUID(as_uuid=True), nullable=False),
    sa.Column('organization_key', postgresql.UUID(as_uuid=True), nullable=False),
    sa.Column('repository_key', postgresql.UUID(as_uuid=True), nullable=False),
    sa.Column('source_commit_id', sa.String(), nullable=False),
    sa.Column('commit_message', sa.Text(), nullable=True),
    sa.Column('committer_contributor_name', sa.String(), nullable=True),
    sa.Column('committer_contributor_key', postgresql.UUID(as_uuid=True), nullable=True),
    sa.Column('commit_date', sa.DateTime(), nullable=False),
    sa.Column('commit_date_tz_offset', sa.Integer(), nullable=True),
    sa.Column('committer_contributor_id', sa.Integer(), nullable=False),
    sa.Column('author_contributor_name', sa.String(), nullable=True),
    sa.Column('author_contributor_key', postgresql.UUID(as_uuid=True), nullable=True),
    sa.Column('author_date', sa.DateTime(), nullable=True),
    sa.Column('author_date_tz_offset', sa.Integer(), nullable=True),
    sa.Column('author_contributor_id', sa.Integer(), nullable=False),
    sa.Column('is_orphan', sa.Boolean(), nullable=True),
    sa.Column('created_at', sa.DateTime(), nullable=True),
    sa.Column('created_on_branch', sa.String(), nullable=True),
    sa.Column('num_parents', sa.Integer(), nullable=True),
    sa.Column('parents', postgresql.ARRAY(sa.String()), nullable=True),
    sa.Column('stats', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    sa.ForeignKeyConstraint(['author_contributor_id'], ['analytics.contributors.id'], ),
    sa.ForeignKeyConstraint(['committer_contributor_id'], ['analytics.contributors.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('repository_key', 'source_commit_id'),
    schema='analytics'
    )
    op.create_index(op.f('ix_analytics_commits_author_contributor_id'), 'commits', ['author_contributor_id'], unique=False, schema='analytics')
    op.create_index('ix_analytics_commits_author_contributor_key', 'commits', ['author_contributor_key', 'committer_contributor_key'], unique=False, schema='analytics')
    op.create_index(op.f('ix_analytics_commits_commit_date'), 'commits', ['commit_date'], unique=False, schema='analytics')
    op.create_index(op.f('ix_analytics_commits_committer_contributor_id'), 'commits', ['committer_contributor_id'], unique=False, schema='analytics')
    op.create_index(op.f('ix_analytics_commits_key'), 'commits', ['key'], unique=True, schema='analytics')
    op.create_index(op.f('ix_analytics_commits_organization_key'), 'commits', ['organization_key'], unique=False, schema='analytics')
    op.create_index(op.f('ix_analytics_commits_repository_key'), 'commits', ['repository_key'], unique=False, schema='analytics')



def downgrade():
    op.drop_index(op.f('ix_analytics_commits_repository_key'), table_name='commits', schema='analytics')
    op.drop_index(op.f('ix_analytics_commits_organization_key'), table_name='commits', schema='analytics')
    op.drop_index(op.f('ix_analytics_commits_key'), table_name='commits', schema='analytics')
    op.drop_index(op.f('ix_analytics_commits_committer_contributor_id'), table_name='commits', schema='analytics')
    op.drop_index(op.f('ix_analytics_commits_commit_date'), table_name='commits', schema='analytics')
    op.drop_index('ix_analytics_commits_author_contributor_key', table_name='commits', schema='analytics')
    op.drop_index(op.f('ix_analytics_commits_author_contributor_id'), table_name='commits', schema='analytics')
    op.drop_table('commits', schema='analytics')
    op.drop_index('ix_analytics_contributors_key_alias_for', table_name='contributors', schema='analytics')
    op.drop_table('contributors', schema='analytics')

