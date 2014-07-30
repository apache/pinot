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
  private long _requestId;
  private Collection<Long> _searchPartitions;

  public Query getQuery() {
    return _query;
  }

  public void setQuery(Query query) {
    _query = query;
  }

  public long getRequestId() {
    return _requestId;
  }

  public void setRequestId(long requestId) {
    _requestId = requestId;
  }

  public Collection<Long> getSearchPartitions() {
    return _searchPartitions;
  }

  public void setSearchPartitions(Collection<Long> searchPartitions) {
    _searchPartitions = searchPartitions;
  }


}
