"""ff_add_flow_efficiency_dashboards

Revision ID: bd6a648d9d9f
Revises: b2732636b7d4
Create Date: 2022-11-08 00:42:12.470559

"""
from alembic import op
from sqlalchemy import orm

from polaris.analytics.db.utils import create_feature_flag_with_default_enablements, delete_feature_flag

# revision identifiers, used by Alembic.
revision = 'bd6a648d9d9f'
down_revision = 'b2732636b7d4'
branch_labels = None
depends_on = None


def upgrade():
    session = orm.Session(bind=op.get_bind())
    create_feature_flag_with_default_enablements('projects.flow_efficiency_dashboards', join_this=session)
    session.commit()


def downgrade():
    session = orm.Session(bind=op.get_bind())
    delete_feature_flag('projects.flow_efficiency_dashboards', join_this=session)
    session.commit()
