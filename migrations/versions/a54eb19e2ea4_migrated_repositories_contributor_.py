"""Migrated repositories_contributor_aliases table from repos

Revision ID: a54eb19e2ea4
Revises: 9bb06cf6c08e
Create Date: 2019-01-09 21:53:52.346254

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a54eb19e2ea4'
down_revision = '9bb06cf6c08e'
branch_labels = None
depends_on = None


def upgrade():

    op.create_table('repositories_contributor_aliases',
    sa.Column('repository_id', sa.Integer(), nullable=False),
    sa.Column('contributor_alias_id', sa.Integer(), nullable=False),
    sa.Column('earliest_commit', sa.DateTime(), nullable=True),
    sa.Column('latest_commit', sa.DateTime(), nullable=True),
    sa.Column('commit_count', sa.Integer(), nullable=True),
    sa.Column('contributor_id', sa.Integer(), nullable=True),
    sa.Column('robot', sa.Boolean(), nullable=True),
    sa.ForeignKeyConstraint(['contributor_alias_id'], ['analytics.contributor_aliases.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['contributor_id'], ['analytics.contributors.id'], ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['repository_id'], ['analytics.repositories.id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('repository_id', 'contributor_alias_id'),
    schema='analytics'
    )
    op.create_index('ix_repositories_contributor_aliasesrepositoryidcontributorid', 'repositories_contributor_aliases', ['repository_id', 'contributor_id'], unique=False, schema='analytics')
    # ### end Alembic commands ###


def downgrade():

    op.drop_index('ix_repositories_contributor_aliasesrepositoryidcontributorid', table_name='repositories_contributor_aliases', schema='analytics')
    op.drop_table('repositories_contributor_aliases', schema='analytics')
    # ### end Alembic commands ###
