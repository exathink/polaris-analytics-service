"""ff_new_flow_dashboard_layout

Revision ID: 32818e4e9156
Revises: f835ec1a7a52
Create Date: 2023-10-29 16:13:43.852518

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import orm
from polaris.analytics.db.utils import create_feature_flag_with_default_enablements, delete_feature_flag

# revision identifiers, used by Alembic.
revision = '32818e4e9156'
down_revision = 'f835ec1a7a52'
branch_labels = None
depends_on = None


def upgrade():
    session = orm.Session(bind=op.get_bind())
    create_feature_flag_with_default_enablements('ui.flow_dashboard_new_layout', join_this=session)
    session.commit()


def downgrade():
    session = orm.Session(bind=op.get_bind())
    delete_feature_flag('ui.flow_dashboard_new_layout', join_this=session)
    session.commit()
