ALTER TABLE analytics.commits ALTER committer_contributor_id DROP NOT NULL;
ALTER TABLE analytics.commits ALTER author_contributor_id DROP NOT NULL;

CREATE OR REPLACE FUNCTION import_commit_facts(repository_key UUID) RETURNS void
AS
$$

    insert into analytics.commits
        (
            key,
            organization_key,
            repository_key,
            source_commit_id,
            commit_date,
            commit_date_tz_offset,
            committer_contributor_key,
            committer_contributor_name,
            author_date,
            author_date_tz_offset,
            author_contributor_key,
            author_contributor_name,
            commit_message,
            num_parents,
            created_at,
            created_on_branch
        )
    select
           gen_random_uuid() as key,
           repositories.organization_key,
           repositories.key as repository_key,
           commits.key as source_commit_id,
           commit_date,
           commit_date_tz_offset,
           committer_contributor_key,
           committer_contributor_name,
           author_date,
           author_date_tz_offset,
           author_contributor_key,
           author_contributor_name,
           commit_message,
           num_parents,
           commits.created_at,
           created_on_branch
    from
      repos.repositories
        inner join repos.commits on commits.repository_id = repositories.id
    where repositories.key = repository_key
    ;

    UPDATE analytics.commits set committer_contributor_id =(
        SELECT id from analytics.contributors WHERE commits.repository_key=repository_key and contributors.key=commits.committer_contributor_key LIMIT 1
    );

    UPDATE analytics.commits set author_contributor_id =(
        SELECT id from analytics.contributors WHERE commits.repository_key=repository_key and contributors.key=commits.author_contributor_key LIMIT 1
    );
$$
LANGUAGE SQL;


ALTER TABLE analytics.commits ALTER committer_contributor_alias_id SET NOT NULL;
ALTER TABLE analytics.commits ALTER author_contributor_alias_id SET NOT NULL;


delete from analytics.contributors;

insert into analytics.contributors (name, key)
select display_name, key from repos.contributor_aliases;


insert into analytics.contributor_aliases (name, key, source_alias, source, contributor_id)
SELECT display_name as name, contributor_aliases.key as key, alias as source_alias, 'vcs' as source, contributors.id as contributor_id from
                                    repos.contributor_aliases inner join analytics.contributors on contributors.key = contributor_aliases.key;




with repo_keys(key) as (select key from repos.repositories)
SELECT import_commit_facts(key) from repo_keys;


ALTER TABLE analytics.commits ALTER committer_contributor_id SET NOT NULL;
ALTER TABLE analytics.commits ALTER author_contributor_id SET NOT NULL;


