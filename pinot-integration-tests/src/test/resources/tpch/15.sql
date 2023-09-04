create view revenue0 (supplier_no, total_revenue) as
select
  l_suppkey,
  sum(
    l_extendedprice * (1 - l_discount)
  )
from
  lineitem
where
  l_shipdate >= '1996-01-01'
  and l_shipdate < '1996-04-01'
group by
  l_suppkey;
