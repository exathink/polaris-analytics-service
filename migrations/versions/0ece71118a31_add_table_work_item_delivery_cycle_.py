"""add_table_work_item_delivery_cycle_contributors

Revision ID: 0ece71118a31
Revises: dbbf66aca87d
Create Date: 2020-04-07 12:35:06.529217

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '0ece71118a31'
down_revision = 'dbbf66aca87d'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('work_item_delivery_cycle_contributors',
    sa.Column('total_lines_as_author', sa.Integer(), nullable=True),
    sa.Column('total_lines_as_reviewer', sa.Integer(), nullable=True),
    sa.Column('delivery_cycle_id', sa.Integer(), nullable=False),
    sa.Column('contributor_alias_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['contributor_alias_id'], ['analytics.contributor_aliases.id'], ),
    sa.ForeignKeyConstraint(['delivery_cycle_id'], ['analytics.work_item_delivery_cycles.delivery_cycle_id'], ),
    sa.PrimaryKeyConstraint('delivery_cycle_id', 'contributor_alias_id'),
    schema='analytics'
    )


def downgrade():
    op.drop_table('work_item_delivery_cycle_contributors', schema='analytics')
