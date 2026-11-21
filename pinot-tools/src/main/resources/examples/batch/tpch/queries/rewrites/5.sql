-- Q5. Original: customer x orders (build 227k), then lineitem (6M) as build side.
-- Filtered orders are joined locally with the replicated customer/nation/region (only ASIA customers survive,
-- ~46k rows) and then with lineitem partition by partition (colocated by order key), so lineitem is never shuffled.
-- The supplier (and the c_nationkey = s_nationkey check) is applied after that reduction, as a local join.
select /*+ joinOptions(left_distribution_type='local', right_distribution_type='local') */
  n_name,
  sum(l_extendedprice * (1 - l_discount)) as revenue
from (
  select /*+ joinOptions(is_colocated_by_join_keys='true') */
    l_suppkey, l_extendedprice, l_discount, c_nationkey, n_name
  from lineitem
    join (
      select /*+ joinOptions(left_distribution_type='local', right_distribution_type='local') */
        o_orderkey, c_nationkey, n_name
      from orders
        join customer /*+ tableOptions(is_replicated='true') */ on c_custkey = o_custkey
        join nation /*+ tableOptions(is_replicated='true') */ on c_nationkey = n_nationkey
        join region /*+ tableOptions(is_replicated='true') */ on n_regionkey = r_regionkey
      where r_name = 'ASIA'
        and o_orderdate >= '1994-01-01'
        and o_orderdate < '1995-01-01'
    ) o on l_orderkey = o_orderkey
) lo
  join supplier /*+ tableOptions(is_replicated='true') */ on l_suppkey = s_suppkey and s_nationkey = c_nationkey
group by n_name
order by revenue desc;
