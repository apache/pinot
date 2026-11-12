-- TPC-H Q15 (top supplier). Spec defines this as CREATE VIEW revenue0 + SELECT + DROP VIEW.
-- Pinot's Calcite dialect rejects CREATE/DROP VIEW, so we inline the view as a CTE.
with revenue0 (supplier_no, total_revenue) as (
  select
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
  from
    lineitem
  where
    l_shipdate >= '1996-01-01'
    and l_shipdate < '1996-04-01'
  group by
    l_suppkey
)
select
  s_suppkey,
  s_name,
  s_address,
  s_phone,
  total_revenue
from
  supplier,
  revenue0
where
  s_suppkey = supplier_no
  and total_revenue = (
    select
      max(total_revenue)
    from
      revenue0
  )
order by
  s_suppkey;
