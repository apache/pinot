package org.apache.pinot.broker.routing.adaptiveserverselector;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;


public class ServerSelectionContext {
  private final Map<String, String> _queryOptions;
  private final List<Integer> _orderedPreferredGroups;

  public ServerSelectionContext(Map<String, String> queryOptions) {
    _queryOptions = queryOptions == null ? Collections.emptyMap() : queryOptions;
    _orderedPreferredGroups = QueryOptionsUtils.getOrderedPreferredReplicas(_queryOptions);
  }

  public Map<String, String> getQueryOptions() {
    return _queryOptions;
  }

  public List<Integer> getOrderedPreferredGroups() {
    return _orderedPreferredGroups;
  }
}
