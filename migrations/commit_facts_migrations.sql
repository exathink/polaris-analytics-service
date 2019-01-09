
insert into analytics.accounts (key, name) select account_key, name from repos.accounts;

insert into analytics.organizations (key, name, public) select organization_key, name, public from repos.organizations;


insert into analytics.accounts_organizations (account_id, organization_id)
select aa.id, ao.id from repos.accounts_organizations
    inner join repos.organizations ro on accounts_organizations.organization_id = ro.id
    inner join repos.accounts ra on accounts_organizations.account_id = ra.id
    inner join analytics.accounts aa on aa.key = ra.account_key
    inner join analytics.organizations ao on ao.key = ro.organization_key;

insert into analytics.projects (key, name, public, properties, organization_id)
select  p.project_key, p.name, p.public, p.properties, ao.id from repos.projects p inner join repos.organizations ro on p.organization_id = ro.id inner join analytics.organizations as ao on ao.key = ro.organization_key

insert into analytics.repositories (key, name, url, public, vendor, properties, earliest_commit, latest_commit, commit_count, organization_id)
select r.key, r.name, r.url, r.public, r.vendor, r.properties, r.earliest_commit, r.latest_commit, r.commit_count, ao.id from
repos.repositories r inner join repos.organizations ro on r.organization_id = ro.id
inner join analytics.organizations ao on ao.key = ro.organization_key;


insert into analytics.projects_repositories (project_id, repository_id)
SELECT ap.id, ar.id from repos.projects_repositories
inner join repos.projects rp on projects_repositories.project_id = rp.id
inner join repos.repositories rr on projects_repositories.repository_id = rr.id
inner join analytics.projects ap on ap.key = rp.project_key
inner join analytics.repositories ar on ar.key = rr.key;

insert into analytics.contributors (name, key)
select display_name, key from repos.contributor_aliases;


insert into analytics.contributor_aliases (name, key, source_alias, robot, source, contributor_id)
SELECT display_name as name, contributor_aliases.key as key, alias as source_alias, robot, 'vcs' as source, contributors.id as contributor_id from
repos.contributor_aliases
inner join analytics.contributors on contributors.key = contributor_aliases.key;





insert into analytics.repositories_contributor_aliases
    (repository_id, contributor_alias_id, contributor_id, earliest_commit, latest_commit, commit_count, robot)
SELECT ar.id as repository_id, aca.id as contributor_alias_id, aca.contributor_id as contributor_id,
rrca.earliest_commit, rrca.latest_commit, rrca.commit_count, rca.robot
from repos.repositories_contributor_aliases rrca
inner join repos.repositories rr on rrca.repository_id = rr.id
inner join analytics.repositories ar on rr.key = ar.key
inner join repos.contributor_aliases rca on rrca.contributor_alias_id = rca.id
inner join analytics.contributor_aliases aca on rca.key=aca.key

select repos.repositories.id, repos.repositories.name from repos.repositories left join analytics.repositories on repos.repositories.key=analytics.repositories.key
where analytics.repositories.id is NULL

update analytics.repositories
set
    earliest_commit = t.earliest_commit,
    latest_commit = t.latest_commit,
    commit_count = t.commit_count
FROM (SELECT min(repositories.key::text) as key,
             min(commit_date) as earliest_commit,
             max(commit_date) as latest_commit,
             count(commits.id)        as commit_count
      from repos.commits
      inner join repos.repositories on commits.repository_id = repositories.id
      group by repositories.id) as t
WHERE t.key = analytics.repositories.key::text




