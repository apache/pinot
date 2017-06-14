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

import org.apache.commons.configuration.Configuration;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;


public interface RoutingTableSelector {
  /**
   * Method to determine whether low-level kafka consumer segments should be used for routing.
   *
   * @param realtimeTableName name of the realtime table (e.g. tableName_REALTIME)
   * @return
   */
  boolean shouldUseLLCRouting(final String realtimeTableName);

  void init(Configuration configuration, ZkHelixPropertyStore<ZNRecord> propertyStore);

  void registerTable(String realtimeTableName);
}
