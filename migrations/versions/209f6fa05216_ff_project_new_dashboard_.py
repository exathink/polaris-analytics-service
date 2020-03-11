"""create_feature_flag_project_new_dashboard

Revision ID: 209f6fa05216
Revises: da723f3ca403
Create Date: 2020-03-07 17:54:41.585447

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import orm

from polaris.analytics.db.utils import create_feature_flag_with_default_enablements, delete_feature_flag

# revision identifiers, used by Alembic.
revision = '209f6fa05216'
down_revision = 'da723f3ca403'
branch_labels = None
depends_on = None


def upgrade():
    session = orm.Session(bind=op.get_bind())
    create_feature_flag_with_default_enablements('projects.new-dashboard', join_this=session)
    session.commit()


def downgrade():
    session = orm.Session(bind=op.get_bind())
    delete_feature_flag('projects.new-dashboard', join_this=session)
    session.commit()

