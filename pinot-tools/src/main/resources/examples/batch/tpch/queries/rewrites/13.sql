-- Q13. Original built a 1.5M-row hash table on orders for the outer join. Count orders per customer first
-- (1.5M -> 100k rows, partially aggregated at the leaf), then outer-join customer with those counts.
-- count(o_orderkey) of an unmatched customer is 0, hence the coalesce.
select
  c_count,
  count(*) as custdist
from (
  select /*+ joinOptions(join_strategy='hash') */
    c_custkey,
    coalesce(o_count, 0) as c_count
  from customer
    left outer join (
      select o_custkey, count(o_orderkey) as o_count
      from orders
      where o_comment not like '%special%requests%'
      group by o_custkey
    ) o on c_custkey = o_custkey
) as c_orders
group by c_count
order by custdist desc, c_count desc;
