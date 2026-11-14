-- Q4. The EXISTS became a dynamic-broadcast semi join that broadcast 3.8M lineitem rows to the orders leaf.
-- Instead join lineitem with the filtered orders partition by partition (both are colocated by order key) and
-- de-duplicate per order in the same partition (o_orderkey is unique, so one row per order matches EXISTS).
select
  o_orderpriority,
  count(*) as order_count
from (
  select /*+ joinOptions(is_colocated_by_join_keys='true'), aggOptions(is_partitioned_by_group_by_keys='true') */
    o_orderkey, o_orderpriority
  from lineitem
    join orders on l_orderkey = o_orderkey
  where o_orderdate >= '1993-07-01'
    and o_orderdate < '1993-10-01'
    and l_commitdate < l_receiptdate
  group by o_orderkey, o_orderpriority
) t
group by o_orderpriority
order by o_orderpriority;
