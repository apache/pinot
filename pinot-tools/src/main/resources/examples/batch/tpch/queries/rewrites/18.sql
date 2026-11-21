-- Q18. Original built hash tables on orders (1.5M) and lineitem (6M). The IN subquery already computes
-- sum(l_quantity) per order, which is what the outer query re-aggregates, so reuse it. lineitem is partitioned by
-- order key, so that aggregation is complete within each partition (no shuffle), and the 57 large orders are
-- joined with orders partition by partition. Only the matching orders are joined with the replicated customer.
select /*+ joinOptions(left_distribution_type='local', right_distribution_type='local') */
  c_name,
  c_custkey,
  o_orderkey,
  o_orderdate,
  o_totalprice,
  sum_quantity
from (
  select /*+ joinOptions(is_colocated_by_join_keys='true') */
    o_orderkey, o_orderdate, o_totalprice, o_custkey, sum_quantity
  from orders
    join (
      select /*+ aggOptions(is_partitioned_by_group_by_keys='true') */ l_orderkey, sum(l_quantity) as sum_quantity
      from lineitem
      group by l_orderkey
      having sum(l_quantity) > 300
    ) big on o_orderkey = l_orderkey
) ob
  join customer /*+ tableOptions(is_replicated='true') */ on c_custkey = o_custkey
order by o_totalprice desc, o_orderdate
limit
  100;
