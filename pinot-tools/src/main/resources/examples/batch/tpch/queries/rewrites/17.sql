-- Q17. The correlated avg subquery aggregated all 6M lineitem rows (200k groups) and scanned lineitem twice.
-- The subquery ranges over exactly the lineitem rows of the same part, which are the rows produced by the
-- lineitem x part join, so it becomes a window AVG partitioned by part over ~6k rows.
select
  sum(l_extendedprice) / 7.0 as avg_yearly
from (
  select /*+ joinOptions(left_distribution_type='local', right_distribution_type='local') */
    l_extendedprice,
    l_quantity,
    avg(l_quantity) over (partition by l_partkey) as avg_quantity
  from lineitem
    join part /*+ tableOptions(is_replicated='true') */ on p_partkey = l_partkey
  where p_brand = 'Brand#23'
    and p_container = 'MED BOX'
) t
where l_quantity < 0.2 * avg_quantity;
