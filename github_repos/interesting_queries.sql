select name, top_10 as "top 10 appearances", top_10p as "top 10 appearances (%)"
from (
    select l.id, l.name as name, count(rl.repo_id) as top_10,
           (round(count(rl.repo_id)::decimal / (select count(*) from repositories)::decimal * 100, 3))::text || '%' as top_10p
    from languages l
    join repo_languages rl on rl.lang_id = l.id
    group by l.id
    order by count(rl.repo_id) asc
) subq;

select count(*) from repositories;

select count(*) from owners;

select round((select count(*) from repositories)::decimal / (select count(*) from owners)::decimal, 3)
as "repos per owner";

select count(*) as "fully expanded repos" from (
    select r.id as id from repositories r
    where not exists (select 1 from repo_errors re where r.id = re.repo_id)
    and not exists (select 1 from repos_todo rt where r.id = rt.repo_id)
) subq;
