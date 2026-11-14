-- Q20. The correlated sum aggregated 909k lineitem rows (183k groups) before joining 8.5k partsupp rows.
-- Both partsupp and lineitem are now restricted to 'forest%' parts with an IN semi join directly over each
-- scan (default dynamic broadcast: the 2k part keys are pushed into the leaf filter), so only ~10k lineitem
-- rows are aggregated before joining partsupp on both keys. A partsupp row without lineitem has a NULL sum in
-- the original (the comparison is not true) and no join partner here: same result.
-- No join hint on purpose: a hint would also apply to the IN semi joins and turn them into shuffled hash joins.
select
  s_name,
  s_address
from supplier, nation
where s_suppkey in (
    select ps_suppkey
    from (
      select ps_partkey, ps_suppkey, ps_availqty
      from partsupp
      where ps_partkey in (select p_partkey from part where p_name like 'forest%')
    ) ps
      join (
        select l_partkey, l_suppkey, sum(l_quantity) as sum_quantity
        from lineitem
        where l_partkey in (select p_partkey from part where p_name like 'forest%')
          and l_shipdate >= '1994-01-01'
          and l_shipdate < '1995-01-01'
        group by l_partkey, l_suppkey
      ) l on l_partkey = ps_partkey and l_suppkey = ps_suppkey
    where ps_availqty > 0.5 * sum_quantity
  )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by s_name;
