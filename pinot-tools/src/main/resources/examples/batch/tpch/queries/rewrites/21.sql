-- Q21. The EXISTS / NOT EXISTS subqueries only look at other lines of the same order. For a late line l1 of
-- supplier s they hold exactly when the order has at least two distinct suppliers (min <> max) and s is the only
-- supplier with late lines (the min and max supplier over late lines are equal; l1 itself is late, so that
-- supplier is s). Both are computed with one aggregation per order, which is complete inside each order key
-- partition. l1 is restricted to 'SAUDI ARABIA' suppliers first (local joins with replicated supplier/nation),
-- and joined with those orders and with orders partition by partition.
select /*+ joinOptions(is_colocated_by_join_keys='true') */
  s_name,
  count(*) as numwait
from (
  select /*+ joinOptions(left_distribution_type='local', right_distribution_type='local') */
    l_orderkey, s_name
  from lineitem
    join supplier /*+ tableOptions(is_replicated='true') */ on s_suppkey = l_suppkey
    join nation /*+ tableOptions(is_replicated='true') */ on s_nationkey = n_nationkey
  where n_name = 'SAUDI ARABIA'
    and l_receiptdate > l_commitdate
) l1
  join (
    select /*+ aggOptions(is_partitioned_by_group_by_keys='true') */
      l_orderkey as w_orderkey
    from lineitem
    group by l_orderkey
    having min(l_suppkey) <> max(l_suppkey)
      and min(case when l_receiptdate > l_commitdate then l_suppkey else 2147483647 end)
        = max(case when l_receiptdate > l_commitdate then l_suppkey else -1 end)
  ) w on l1.l_orderkey = w_orderkey
  join orders on o_orderkey = l1.l_orderkey
where o_orderstatus = 'F'
group by
  s_name
order by
  numwait desc,
  s_name
limit
  100;
