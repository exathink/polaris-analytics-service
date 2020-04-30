"""create_table_work_item_source_file_changes

Revision ID: 5afd72f9d707
Revises: b5af1576746c
Create Date: 2020-04-23 13:42:52.702313

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5afd72f9d707'
down_revision = 'b5af1576746c'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('work_item_source_file_changes',
    sa.Column('id', sa.Integer(), nullable=False),
    sa.Column('work_item_id', sa.Integer(), nullable=False),
    sa.Column('delivery_cycle_id', sa.Integer(), nullable=True),
    sa.Column('repository_id', sa.Integer(), nullable=False),
    sa.Column('source_file_id', sa.BigInteger(), nullable=False),
    sa.Column('commit_id', sa.BigInteger(), nullable=False),
    sa.Column('source_commit_id', sa.String(), nullable=False),
    sa.Column('commit_date', sa.DateTime(), nullable=False),
    sa.Column('committer_contributor_alias_id', sa.Integer(), nullable=False),
    sa.Column('author_contributor_alias_id', sa.Integer(), nullable=False),
    sa.Column('created_on_branch', sa.String(), nullable=True),
    sa.Column('file_action', sa.String(), nullable=False),
    sa.Column('total_lines_changed', sa.Integer(), nullable=True),
    sa.Column('total_lines_deleted', sa.Integer(), nullable=True),
    sa.Column('total_lines_added', sa.Integer(), nullable=True),
    sa.ForeignKeyConstraint(['commit_id'], ['analytics.commits.id'], ),
    sa.ForeignKeyConstraint(['delivery_cycle_id'], ['analytics.work_item_delivery_cycles.delivery_cycle_id'], ),
    sa.ForeignKeyConstraint(['repository_id'], ['analytics.repositories.id'], ),
    sa.ForeignKeyConstraint(['source_file_id'], ['analytics.source_files.id'], ),
    sa.ForeignKeyConstraint(['work_item_id'], ['analytics.work_items.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('work_item_id', 'delivery_cycle_id', 'repository_id', 'commit_id', 'source_file_id', name=None),
    schema='analytics'
    )


def downgrade():
    op.drop_table('work_item_source_file_changes', schema='analytics')
