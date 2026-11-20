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
package org.apache.pinot.tsdb.planner.physical;

import java.util.Map;


/// What a [TimeSeriesRoutingCustomizer] sees of the time-series query being planned. One instance per query, used by
/// one planning thread.
public interface TimeSeriesRoutingContext {
  /// The query options of the request, read-only.
  Map<String, String> getRequestQueryOptions();

  /// Query options added while routing the leaves of this query, read-only. They are sent to the servers with every
  /// leaf of the query, replacing a leaf's own option with the same key. A later leaf sees what an earlier one added.
  Map<String, String> getRoutingQueryOptions();

  /// Adds or replaces a routing query option. It applies to every leaf of the query, so a value that differs per
  /// table should be keyed by table inside the value.
  void putRoutingQueryOption(String key, String value);
}
