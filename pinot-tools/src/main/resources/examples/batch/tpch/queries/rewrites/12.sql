-- Q12. Orientation was already right (small filtered lineitem as build side), but 1.5M orders rows were
-- shuffled. orders and lineitem are colocated by order key, so join them partition by partition.
select /*+ joinOptions(is_colocated_by_join_keys='true') */
  l_shipmode,
  sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,
  sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count
from orders
  join lineitem on o_orderkey = l_orderkey
where l_shipmode in ('MAIL', 'SHIP')
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= '1994-01-01'
  and l_receiptdate < '1995-01-01'
group by l_shipmode
order by l_shipmode;
