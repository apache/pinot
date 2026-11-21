-- Q3. Original built hash tables on orders (727k) and lineitem (3.2M). The filtered orders are joined with the
-- replicated customer locally (147k rows). orders and lineitem are partitioned and colocated by order key, so that
-- result is joined with lineitem partition by partition: the 3.2M lineitem rows are never shuffled.
select /*+ joinOptions(is_colocated_by_join_keys='true') */
  l_orderkey,
  sum(l_extendedprice * (1 - l_discount)) as revenue,
  o_orderdate,
  o_shippriority
from lineitem
  join (
    select /*+ joinOptions(left_distribution_type='local', right_distribution_type='local') */
      o_orderkey, o_orderdate, o_shippriority
    from orders
      join customer /*+ tableOptions(is_replicated='true') */ on c_custkey = o_custkey
    where c_mktsegment = 'BUILDING'
      and o_orderdate < '1995-03-15'
  ) o on l_orderkey = o_orderkey
where l_shipdate > '1995-03-15'
group by l_orderkey, o_orderdate, o_shippriority
order by revenue desc, o_orderdate
limit
  10;
