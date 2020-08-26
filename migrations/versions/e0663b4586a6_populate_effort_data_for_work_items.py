"""populate_effort_data_for_work_items

Revision ID: e0663b4586a6
Revises: 64452fc7e7f8
Create Date: 2020-08-26 19:13:21.122864

"""
from alembic import op
import sqlalchemy as sa
from polaris.common import db
from polaris.utils.collections import dict_to_object
from polaris.analytics.db.impl.delivery_cycle_tracking import \
    compute_work_items_implementation_effort, \
    compute_delivery_cycles_implementation_effort
from polaris.analytics.db.model import work_items

# revision identifiers, used by Alembic.
revision = 'e0663b4586a6'
down_revision = '64452fc7e7f8'
branch_labels = None
depends_on = None


def upgrade():
    session = dict_to_object(dict(connection=lambda: op.get_bind()))

    print("Updating effort for work items")
    updated = compute_work_items_implementation_effort(
        session,
        sa.select([work_items.c.key.label('work_item_key')]).alias()
    )
    print(f'{updated} work items updated')

    print("Updating effort for delivery cycles")
    updated = compute_delivery_cycles_implementation_effort(
        session,
        sa.select([work_items.c.key.label('work_item_key')]).alias()
    )
    print(f'{updated} delivery cycles updated')


def downgrade():
    op.execute("update analytics.work_items set effort=null")
    op.execute("update analytics.work_item_delivery_cycles set effort=null")
