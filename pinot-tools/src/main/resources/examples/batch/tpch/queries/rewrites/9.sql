-- Q9. The original FROM order starts with a part x supplier cross join. Instead restrict lineitem to the
-- '%green%' parts (~5%) through a local join with the replicated part table and attach the supplier nation the
-- same way, join orders partition by partition (colocated by order key), and only then join partsupp on both
-- keys. partsupp is pre-filtered to the same parts (a dynamic broadcast semi join over its scan) and broadcast.
select
  nation,
  o_year,
  sum(amount) as sum_profit
from (
  select /*+ joinOptions(left_distribution_type='local', right_distribution_type='broadcast') */
    nation,
    o_year,
    l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
  from (
    select /*+ joinOptions(is_colocated_by_join_keys='true') */
      l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, nation,
      substring(o_orderdate, 1, 4) as o_year
    from (
      select /*+ joinOptions(left_distribution_type='local', right_distribution_type='local') */
        l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, n_name as nation
      from lineitem
        join part /*+ tableOptions(is_replicated='true') */ on p_partkey = l_partkey
        join supplier /*+ tableOptions(is_replicated='true') */ on s_suppkey = l_suppkey
        join nation /*+ tableOptions(is_replicated='true') */ on s_nationkey = n_nationkey
      where p_name like '%green%'
    ) l
      join orders on o_orderkey = l_orderkey
  ) lo
    join (
      select ps_partkey, ps_suppkey, ps_supplycost
      from partsupp
      where ps_partkey in (select p_partkey from part where p_name like '%green%')
    ) ps on ps_partkey = l_partkey and ps_suppkey = l_suppkey
) as profit
group by
  nation,
  o_year
order by
  nation,
  o_year desc;
