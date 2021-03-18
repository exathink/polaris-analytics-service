"""ff_projects_value_dashboard

Revision ID: 2c6b8972d8db
Revises: fe88b8d6b964
Create Date: 2021-03-17 20:14:07.919139

"""
from alembic import op
import sqlalchemy as sa

from sqlalchemy import orm

from polaris.analytics.db.utils import create_feature_flag_with_default_enablements, delete_feature_flag

# revision identifiers, used by Alembic.
revision = '2c6b8972d8db'
down_revision = 'fe88b8d6b964'
branch_labels = None
depends_on = None


def upgrade():
    session = orm.Session(bind=op.get_bind())
    create_feature_flag_with_default_enablements('projects.value-dashboard', join_this=session)
    session.commit()


def downgrade():
    session = orm.Session(bind=op.get_bind())
    delete_feature_flag('projects.value-dashboard', join_this=session)
    session.commit()
