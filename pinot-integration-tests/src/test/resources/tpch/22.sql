select
  s_name,
  s_address
from
  supplier,
  nation
where
  s_suppkey in (
    select
      ps_suppkey
    from
      partsupp
    where
      ps_partkey in (
        select
          p_partkey
        from
          part
        where
          p_name like 'forest%'
      )
      and ps_availqty > (
        select
          CAST(0.5 * SUM(l_quantity) AS DOUBLE)
        from
          lineitem
        where
          l_partkey = ps_partkey
          and l_suppkey = ps_suppkey
          and l_shipdate >= 757362600
          and l_shipdate < 788898600
      )
  )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
order by
  s_name;
