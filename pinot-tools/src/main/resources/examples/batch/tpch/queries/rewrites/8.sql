-- Q8. Original FROM order produced a part x supplier cross join (14.5M rows) probed against a 6M-row lineitem
-- hash table. Left side: AMERICA orders (orders + replicated customer/nation/region, joined locally).
-- Right side: lineitem restricted to the target part type via the replicated part table, enriched with the
-- supplier nation (replicated supplier/nation, local joins). Both sides keep the order key partitioning of their
-- fact table, so they are joined partition by partition.
select
  o_year,
  sum(case when nation = 'BRAZIL' then volume else 0 end) / sum(volume) as mkt_share
from (
  select /*+ joinOptions(is_colocated_by_join_keys='true') */
    substring(o_orderdate, 1, 4) as o_year,
    volume,
    nation
  from (
    select /*+ joinOptions(left_distribution_type='local', right_distribution_type='local') */
      o_orderkey, o_orderdate
    from orders
      join customer /*+ tableOptions(is_replicated='true') */ on o_custkey = c_custkey
      join nation /*+ tableOptions(is_replicated='true') */ on c_nationkey = n_nationkey
      join region /*+ tableOptions(is_replicated='true') */ on n_regionkey = r_regionkey
    where r_name = 'AMERICA'
      and o_orderdate between '1995-01-01' and '1996-12-31'
  ) o
    join (
      select /*+ joinOptions(left_distribution_type='local', right_distribution_type='local') */
        l_orderkey,
        l_extendedprice * (1 - l_discount) as volume,
        n_name as nation
      from lineitem
        join part /*+ tableOptions(is_replicated='true') */ on p_partkey = l_partkey
        join supplier /*+ tableOptions(is_replicated='true') */ on s_suppkey = l_suppkey
        join nation /*+ tableOptions(is_replicated='true') */ on s_nationkey = n_nationkey
      where p_type = 'ECONOMY ANODIZED STEEL'
    ) l on l_orderkey = o_orderkey
) as all_nations
group by o_year
order by o_year;
