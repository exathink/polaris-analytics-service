"""add_table_work_item_delivery_cycle_contributors

Revision ID: c0d3710486f7
Revises: 38da2ee3c64f
Create Date: 2020-04-13 15:53:05.700656

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c0d3710486f7'
down_revision = '38da2ee3c64f'
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
