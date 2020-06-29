"""ff_project_trend_dashboard

Revision ID: 9d94f91512d8
Revises: 254d4cad11db
Create Date: 2020-06-29 15:14:32.384972

"""
from alembic import op
from sqlalchemy import orm

from polaris.analytics.db.utils import create_feature_flag_with_default_enablements, delete_feature_flag

# revision identifiers, used by Alembic.
revision = '9d94f91512d8'
down_revision = '254d4cad11db'
branch_labels = None
depends_on = None


def upgrade():
    session = orm.Session(bind=op.get_bind())
    create_feature_flag_with_default_enablements('projects.trends-dashboard', join_this=session)
    session.commit()


def downgrade():
    session = orm.Session(bind=op.get_bind())
    delete_feature_flag('projects.trends-dashboard', join_this=session)
    session.commit()