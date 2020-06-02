"""ff_work_item_detail_dashboard

Revision ID: 6ecf5164bae5
Revises: 7b2f203d5bb3
Create Date: 2020-06-02 18:52:02.082557

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy import orm

from polaris.analytics.db.utils import create_feature_flag_with_default_enablements, delete_feature_flag

# revision identifiers, used by Alembic.
revision = '6ecf5164bae5'
down_revision = '7b2f203d5bb3'
branch_labels = None
depends_on = None


def upgrade():
    session = orm.Session(bind=op.get_bind())
    create_feature_flag_with_default_enablements('work_items.detail-dashboard', join_this=session)
    session.commit()


def downgrade():
    session = orm.Session(bind=op.get_bind())
    delete_feature_flag('work_items.detail-dashboard', join_this=session)
    session.commit()
