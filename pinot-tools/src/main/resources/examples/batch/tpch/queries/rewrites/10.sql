-- Q10. Original: customer x orders, then lineitem (1.5M) as build side, grouping by 7 wide customer columns.
-- Join lineitem with the filtered orders partition by partition (colocated by order key), aggregate by o_custkey
-- first (every grouped customer column is functionally dependent on c_custkey), then attach customer/nation
-- (replicated, local) to the 38k aggregated rows only.
select /*+ joinOptions(left_distribution_type='local', right_distribution_type='local') */
  c_custkey,
  c_name,
  revenue,
  c_acctbal,
  n_name,
  c_address,
  c_phone,
  c_comment
from (
  select /*+ joinOptions(is_colocated_by_join_keys='true') */
    o_custkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue
  from lineitem
    join orders on l_orderkey = o_orderkey
  where o_orderdate >= '1993-10-01'
    and o_orderdate < '1994-01-01'
    and l_returnflag = 'R'
  group by o_custkey
) r
  join customer /*+ tableOptions(is_replicated='true') */ on c_custkey = o_custkey
  join nation /*+ tableOptions(is_replicated='true') */ on c_nationkey = n_nationkey
order by revenue desc
limit
  20;
