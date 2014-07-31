package com.linkedin.pinot.common.utils.request;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.FilterQuery;
import com.linkedin.pinot.common.request.FilterQueryMap;

public class RequestUtils {

  /**
   * Generates thrift compliant filterQuery and populate it in the broker request
   * @param filterQueryTree
   * @param request
   */
  public static void generateFilterFromTree(FilterQueryTree filterQueryTree, BrokerRequest request)
  {
    Map<Integer, FilterQuery> filterQueryMap = new HashMap<Integer, FilterQuery>();
    FilterQuery root = traverseFilterQueryAndPopulateMap(filterQueryTree, filterQueryMap);
    request.setFilterQuery(root);
    FilterQueryMap mp = new FilterQueryMap();
    mp.setFilterQueryMap(filterQueryMap);
    request.setFilterSubQueryMap(mp);
  }

  private static FilterQuery traverseFilterQueryAndPopulateMap(FilterQueryTree tree,Map<Integer, FilterQuery> filterQueryMap )
  {
    List<Integer> f = new ArrayList<Integer>();
    for (FilterQueryTree c : tree.getChildren())
    {
      f.add(c.getId());
      FilterQuery q = traverseFilterQueryAndPopulateMap(c, filterQueryMap);
      filterQueryMap.put(c.getId(), q);
    }

    FilterQuery query = new FilterQuery();
    query.setColumn(tree.getColumn());
    query.setId(tree.getId());
    query.setNestedFilterQueryIds(f);
    query.setOperator(tree.getOperator());
    query.setValue(tree.getValue());
    return query;
  }
  /**
   * Generate FilterQueryTree from Broker Request
   * @param request Broker Request
   * @return
   */
  public static FilterQueryTree generateFilterQueryTree(BrokerRequest request)
  {
    FilterQueryTree root = null;

    FilterQuery q = request.getFilterQuery();

    if ( null != q)
    {
      root = buildFilterQuery(q.getId(), request.getFilterSubQueryMap().getFilterQueryMap());
    }

    return root;
  }

  private static FilterQueryTree buildFilterQuery(Integer id, Map<Integer, FilterQuery> queryMap)
  {
    FilterQuery q = queryMap.get(id);

    List<Integer> children = q.getNestedFilterQueryIds();

    List<FilterQueryTree> c = null;
    if ( null != children)
    {
      c = new ArrayList<FilterQueryTree>();
      for (Integer i : children)
      {
        FilterQueryTree t = buildFilterQuery(i, queryMap);
        c.add(t);
      }
    }

    FilterQueryTree q2 = new FilterQueryTree(id, q.getColumn(), q.getValue(), q.getOperator(), c);
    return q2;
  }
}
