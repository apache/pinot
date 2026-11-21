-- Q19. Every OR branch requires the same join key, l_shipmode and l_shipinstruct, and each one bounds l_quantity,
-- p_size, p_brand and p_container, so those implied conditions are also written outside the OR. They filter
-- lineitem at the scan (~2% survive) and part before the local join with the replicated part table. The full OR
-- is kept, so the result is unchanged.
select /*+ joinOptions(left_distribution_type='local', right_distribution_type='local') */
  sum(l_extendedprice * (1 - l_discount)) as revenue
from lineitem
  join part /*+ tableOptions(is_replicated='true') */ on p_partkey = l_partkey
where l_shipmode in ('AIR', 'AIR REG')
  and l_shipinstruct = 'DELIVER IN PERSON'
  and l_quantity >= 1
  and l_quantity <= 30
  and p_size between 1 and 15
  and p_brand in ('Brand#12', 'Brand#23', 'Brand#34')
  and (
    (
      p_brand = 'Brand#12'
      and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
      and l_quantity >= 1
      and l_quantity <= 1 + 10
      and p_size between 1 and 5
    )
    or (
      p_brand = 'Brand#23'
      and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
      and l_quantity >= 10
      and l_quantity <= 10 + 10
      and p_size between 1 and 10
    )
    or (
      p_brand = 'Brand#34'
      and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
      and l_quantity >= 20
      and l_quantity <= 20 + 10
      and p_size between 1 and 15
    )
  );
