-- Q2. Original FROM order (part, supplier, ...) produced a part x supplier cross join (7.5M rows).
-- Start from partsupp and join the replicated part/supplier/nation/region tables locally. The correlated
-- min(ps_supplycost) subquery ranges over exactly the rows of this join for the same part, so it becomes a
-- window MIN partitioned by part.
select
  s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
from (
  select /*+ joinOptions(left_distribution_type='local', right_distribution_type='local') */
    s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment, ps_supplycost,
    min(ps_supplycost) over (partition by p_partkey) as min_supplycost
  from partsupp
    join part /*+ tableOptions(is_replicated='true') */ on p_partkey = ps_partkey
    join supplier /*+ tableOptions(is_replicated='true') */ on s_suppkey = ps_suppkey
    join nation /*+ tableOptions(is_replicated='true') */ on s_nationkey = n_nationkey
    join region /*+ tableOptions(is_replicated='true') */ on n_regionkey = r_regionkey
  where p_size = 15
    and p_type like '%BRASS'
    and r_name = 'EUROPE'
) t
where ps_supplycost = min_supplycost
order by s_acctbal desc, n_name, s_name, p_partkey
limit
  100;
