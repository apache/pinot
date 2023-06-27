select
  sum(l_extendedprice * l_discount) as revenue
from
  lineitem
where
  l_shipdate >= 757382400
  and l_shipdate < 788918400
  and l_discount between.06 - 0.01
  and.06 + 0.01
  and l_quantity < 24;
