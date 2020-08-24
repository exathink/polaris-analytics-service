"""add_delivery_cycle_id_to_work_items_commits

Revision ID: e8e27c64c692
Revises: e940baf6688e
Create Date: 2020-08-23 22:52:21.745795

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'e8e27c64c692'
down_revision = 'e940baf6688e'
branch_labels = None
depends_on = None


def populate_delivery_cycle_id():
    op.execute(
        """
        with work_items_commits_delivery_cycles as (
            with cycle_windows as (
                select work_items.id,
                       display_id,
                       name,
                       delivery_cycle_id,
                       start_date,
                       end_date,
                       lead(start_date) over (partition by work_items.id order by start_date) as next_start
                from analytics.work_items
                         inner join analytics.work_item_delivery_cycles
                                    on work_items.id = work_item_delivery_cycles.work_item_id
            )
            select cycle_windows.id as work_item_id, commits.id as commit_id, cycle_windows.delivery_cycle_id
            from cycle_windows
                     inner join analytics.work_items_commits on cycle_windows.id = work_items_commits.work_item_id
                     inner join analytics.commits on work_items_commits.commit_id = commits.id
            where start_date <= commit_date
              and (next_start is null or commit_date < next_start)
        )
        update analytics.work_items_commits set delivery_cycle_id = work_items_commits_delivery_cycles.delivery_cycle_id from work_items_commits_delivery_cycles
        where work_items_commits.commit_id=work_items_commits_delivery_cycles.commit_id and work_items_commits.work_item_id=work_items_commits_delivery_cycles.work_item_id
        """
    )


def upgrade():
    op.add_column('work_items_commits', sa.Column('delivery_cycle_id', sa.Integer(), nullable=True), schema='analytics')
    op.create_index(op.f('ix_analytics_work_items_commits_delivery_cycle_id'), 'work_items_commits',
                    ['delivery_cycle_id'], unique=False, schema='analytics')
    op.create_foreign_key('analytics.work_items_commit_delivery_cycle_fk', 'work_items_commits', 'work_item_delivery_cycles', ['delivery_cycle_id'],
                          ['delivery_cycle_id'], source_schema='analytics', referent_schema='analytics')

    populate_delivery_cycle_id()



def downgrade():
    op.drop_constraint('analytics.work_items_commit_delivery_cycle_fk', 'work_items_commits', schema='analytics', type_='foreignkey')
    op.drop_index(op.f('ix_analytics_work_items_commits_delivery_cycle_id'), table_name='work_items_commits',
                  schema='analytics')
    op.drop_column('work_items_commits', 'delivery_cycle_id', schema='analytics')
