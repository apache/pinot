create view revenue0 (supplier_no, total_revenue) as
select
  l_suppkey,
  sum(
    l_extendedprice * (1 - l_discount)
  )
from
  lineitem
where
  l_shipdate >= 820434600
  and l_shipdate < 828297000
group by
  l_suppkey;
