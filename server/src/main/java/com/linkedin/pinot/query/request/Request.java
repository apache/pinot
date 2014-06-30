package com.linkedin.pinot.query.request;

import java.io.Serializable;
import java.util.Collection;


/**
 * A request is constructed by requestId, a collection of partitions to
 * search, and a query object.
 * @author xiafu
 *
 */
public class Request implements Serializable {
  private Query _query;
  private int _requestId;
  private Collection<Integer> _searchPartitions;

  public Query getQuery() {
    return _query;
  }

  public void setQuery(Query query) {
    _query = query;
  }

  public int getRequestId() {
    return _requestId;
  }

  public void setRequestId(int requestId) {
    _requestId = requestId;
  }

  public Collection<Integer> getSearchPartitions() {
    return _searchPartitions;
  }

  public void setSearchPartitions(Collection<Integer> searchPartitions) {
    _searchPartitions = searchPartitions;
  }

}
