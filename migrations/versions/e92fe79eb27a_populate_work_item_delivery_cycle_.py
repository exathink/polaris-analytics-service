"""populate_work_item_delivery_cycle_contributors

Revision ID: e92fe79eb27a
Revises: c0d3710486f7
Create Date: 2020-04-13 16:02:32.048981

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'e92fe79eb27a'
down_revision = 'c0d3710486f7'
branch_labels = None
depends_on = None


def upgrade():
    # insert into work_item_delivery_cycle_contributors
    op.execute("""
    	WITH work_item_delivery_cycle_contributors AS
        (SELECT work_item_delivery_cycles.delivery_cycle_id AS delivery_cycle_id,
        contributor_aliases.id AS contributor_alias_id,
        SUM(CASE
            WHEN (commits.num_parents = 1 AND
            contributor_aliases.id = commits.author_contributor_alias_id)
            THEN CAST(commits.stats ->> 'lines' as INTEGER)
            ELSE 0 END) as total_lines_as_author,
        SUM(CASE
            WHEN ((commits.num_parents > 1 and contributor_aliases.id = commits.committer_contributor_alias_id) or
            (commits.num_parents = 1 and contributor_aliases.id = commits.committer_contributor_alias_id and
            commits.committer_contributor_alias_id != commits.author_contributor_alias_id))
            THEN CAST(commits.stats ->> 'lines' as INTEGER)
            ELSE 0 END) as total_lines_as_reviewer
        FROM analytics.work_item_delivery_cycles
            JOIN analytics.work_items_commits
            ON analytics.work_item_delivery_cycles.work_item_id = work_items_commits.work_item_id
            JOIN analytics.commits on work_items_commits.commit_id = commits.id
            JOIN analytics.contributor_aliases on commits.author_contributor_alias_id = contributor_aliases.id or commits.committer_contributor_alias_id = contributor_aliases.id
            WHERE analytics.commits.commit_date >= analytics.work_item_delivery_cycles.start_date
            GROUP BY analytics.work_item_delivery_cycles.delivery_cycle_id, analytics.contributor_aliases.id)
            INSERT INTO analytics.work_item_delivery_cycle_contributors(delivery_cycle_id,total_lines_as_author,total_lines_as_reviewer,contributor_alias_id) 
            SELECT delivery_cycle_id, total_lines_as_author, total_lines_as_reviewer, contributor_alias_id from work_item_delivery_cycle_contributors
        """)


def downgrade():
    op.execute("delete from analytics.work_item_delivery_cycle_contributors")
