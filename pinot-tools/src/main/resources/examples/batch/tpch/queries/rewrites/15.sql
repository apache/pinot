-- Q15. revenue0 was expanded twice (two lineitem scans + aggregations). Compute it once, find the max with a
-- window, and join the single winning row with the replicated supplier table locally.
with revenue0 (supplier_no, total_revenue) as (
  select l_suppkey, sum(l_extendedprice * (1 - l_discount))
  from lineitem
  where l_shipdate >= '1996-01-01'
    and l_shipdate < '1996-04-01'
  group by l_suppkey
)
select /*+ joinOptions(left_distribution_type='local', right_distribution_type='local') */
  s_suppkey, s_name, s_address, s_phone, total_revenue
from (
  select supplier_no, total_revenue, max(total_revenue) over () as max_revenue
  from revenue0
) r
  join supplier /*+ tableOptions(is_replicated='true') */ on s_suppkey = supplier_no
where total_revenue = max_revenue
order by s_suppkey;
