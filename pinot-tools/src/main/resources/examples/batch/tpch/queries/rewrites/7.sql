-- Q7. Original built hash tables on lineitem (1.8M) and orders (1.5M) and applied the nation filter only at
-- the end. The OR on (n1, n2) implies the customer nation is in ('FRANCE', 'GERMANY'), so orders are
-- pre-filtered through local joins with replicated customer/nation (~120k rows) and joined with lineitem partition
-- by partition (colocated by order key). Supplier nation is attached after the reduction, with local joins.
select
  supp_nation, cust_nation, l_year, sum(volume) as revenue
from (
  select /*+ joinOptions(left_distribution_type='local', right_distribution_type='local') */
    n_name as supp_nation,
    cust_nation,
    substring(l_shipdate, 1, 4) as l_year,
    l_extendedprice * (1 - l_discount) as volume
  from (
    select /*+ joinOptions(is_colocated_by_join_keys='true') */
      l_suppkey, l_shipdate, l_extendedprice, l_discount, cust_nation
    from lineitem
      join (
        select /*+ joinOptions(left_distribution_type='local', right_distribution_type='local') */
          o_orderkey, n_name as cust_nation
        from orders
          join customer /*+ tableOptions(is_replicated='true') */ on c_custkey = o_custkey
          join nation /*+ tableOptions(is_replicated='true') */ on c_nationkey = n_nationkey
        where n_name in ('FRANCE', 'GERMANY')
      ) o on l_orderkey = o_orderkey
    where l_shipdate between '1995-01-01' and '1996-12-31'
  ) lo
    join supplier /*+ tableOptions(is_replicated='true') */ on s_suppkey = l_suppkey
    join nation /*+ tableOptions(is_replicated='true') */ on s_nationkey = n_nationkey
  where (n_name = 'FRANCE' and cust_nation = 'GERMANY')
     or (n_name = 'GERMANY' and cust_nation = 'FRANCE')
) as shipping
group by supp_nation, cust_nation, l_year
order by supp_nation, cust_nation, l_year;
