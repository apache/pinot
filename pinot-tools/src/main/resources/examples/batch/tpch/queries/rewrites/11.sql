-- Q11. Join partsupp with the replicated supplier/nation tables locally (no shuffle), and compute the scalar
-- threshold with a window over the grouped result instead of re-scanning and re-joining partsupp.
select
  ps_partkey,
  total_cost
from (
  select
    ps_partkey,
    total_cost,
    sum(total_cost) over () as grand_total
  from (
    select /*+ joinOptions(left_distribution_type='local', right_distribution_type='local') */
      ps_partkey,
      sum(ps_supplycost * ps_availqty) as total_cost
    from partsupp
      join supplier /*+ tableOptions(is_replicated='true') */ on ps_suppkey = s_suppkey
      join nation /*+ tableOptions(is_replicated='true') */ on s_nationkey = n_nationkey
    where n_name = 'GERMANY'
    group by ps_partkey
  ) g
) t
where total_cost > grand_total * 0.0001000000
order by total_cost desc;
