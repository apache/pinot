/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.broker.routing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.QuerySource;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * The class <code>RoutingTableLookupRequest</code> encapsulates all the information needed for routing table lookup.
 */
public class RoutingTableLookupRequest {
  private static final String ROUTING_OPTIONS_KEY = "routingOptions";

  private final BrokerRequest _brokerRequest;
  private final String _tableName;
  private final List<String> _routingOptions;

  @Nonnull
  public BrokerRequest getBrokerRequest() {
    return _brokerRequest;
  }

  @Nonnull
  public String getTableName() {
    return _tableName;
  }

  @Nonnull
  public List<String> getRoutingOptions() {
    return _routingOptions;
  }

  public RoutingTableLookupRequest(@Nonnull BrokerRequest brokerRequest) {
    _brokerRequest = brokerRequest;
    _tableName = brokerRequest.getQuerySource().getTableName();

    Map<String, String> debugOptions = brokerRequest.getDebugOptions();
    if (debugOptions == null || !debugOptions.containsKey(ROUTING_OPTIONS_KEY)) {
      _routingOptions = Collections.emptyList();
    } else {
      _routingOptions =
          Splitter.on(',').omitEmptyStrings().trimResults().splitToList(debugOptions.get(ROUTING_OPTIONS_KEY));
    }
  }

  @VisibleForTesting
  public RoutingTableLookupRequest(@Nonnull String tableName) {
    _brokerRequest = new BrokerRequest();
    QuerySource querySource = new QuerySource();
    querySource.setTableName(tableName);
    _brokerRequest.setQuerySource(querySource);

    _tableName = tableName;
    _routingOptions = Collections.emptyList();
  }
}
