"""create_work_items_pull_requests_relationship_table

Revision ID: f1656bdab6ba
Revises: adc8a5c36fa7
Create Date: 2020-07-07 12:40:33.192339

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f1656bdab6ba'
down_revision = 'adc8a5c36fa7'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table('work_items_pull_requests',
    sa.Column('work_item_id', sa.BigInteger(), nullable=False),
    sa.Column('pull_request_id', sa.Integer(), nullable=False),
    sa.ForeignKeyConstraint(['pull_request_id'], ['analytics.pull_requests.id'], ),
    sa.ForeignKeyConstraint(['work_item_id'], ['analytics.work_items.id'], ),
    sa.PrimaryKeyConstraint('work_item_id', 'pull_request_id'),
    schema='analytics'
    )
    op.create_index(op.f('ix_analytics_work_items_pull_requests_pull_request_id'), 'work_items_pull_requests', ['pull_request_id'], unique=False, schema='analytics')
    op.create_index(op.f('ix_analytics_work_items_pull_requests_work_item_id'), 'work_items_pull_requests', ['work_item_id'], unique=False, schema='analytics')

    # Update this migration to restart pull request resolution from scratch
    # Technically, what we are doing here is not cool - updating data in the repos schema in an
    # analytics-service migration, but this is probably the safest and most straightforward way
    # of doing this at this point.
    op.execute("delete from repos.pull_requests")
    op.execute("delete from analytics.pull_requests")


def downgrade():
    op.drop_index(op.f('ix_analytics_work_items_pull_requests_work_item_id'), table_name='work_items_pull_requests', schema='analytics')
    op.drop_index(op.f('ix_analytics_work_items_pull_requests_pull_request_id'), table_name='work_items_pull_requests', schema='analytics')
    op.drop_table('work_items_pull_requests', schema='analytics')
