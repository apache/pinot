/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.broker.routing.adaptiveserverselector;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;


/**
 * This class encapsulates query options and ordered preferred replica groups that influence how
 * servers are selected for query execution.
 */
public class ServerSelectionContext {
  private final Map<String, String> _queryOptions;
  private final List<Integer> _orderedPreferredGroups;

  /**
   * Creates a new server selection context with the given query options.
   * The ordered preferred groups are extracted from the query options using
   * {@link QueryOptionsUtils#getOrderedPreferredReplicas(Map)}.
   *
   * @param queryOptions map of query options that may contain server selection preferences
   */
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
