-- Q16. Join partsupp with the replicated part table locally instead of shuffling 800k partsupp rows.
select
  p_brand, p_type, p_size,
  count(distinct ps_suppkey) as supplier_cnt
from (
  select /*+ joinOptions(left_distribution_type='local', right_distribution_type='local') */
    p_brand, p_type, p_size, ps_suppkey
  from partsupp
    join part /*+ tableOptions(is_replicated='true') */ on p_partkey = ps_partkey
  where p_brand <> 'Brand#45'
    and p_type not like 'MEDIUM POLISHED%'
    and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
) ps
where ps_suppkey not in (
  select s_suppkey from supplier where s_comment like '%Customer%Complaints%'
)
group by p_brand, p_type, p_size
order by supplier_cnt desc, p_brand, p_type, p_size;
