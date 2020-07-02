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
package org.apache.pinot.broker.routing.instanceselector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.utils.HashUtil;


/**
 * Instance selector to balance the number of segments served by each selected server instance.
 * <p>The selection algorithm will always evenly distribute the traffic to all replicas of each segment, and will try
 * to select different replica id for each segment. The algorithm is very light-weight and will do best effort to
 * balance the number of segments served by each selected server instance.
 */
public class BalancedInstanceSelector extends BaseInstanceSelector {

  public BalancedInstanceSelector(String tableNameWithType, BrokerMetrics brokerMetrics) {
    super(tableNameWithType, brokerMetrics);
  }

  @Override
  Map<String, String> select(List<String> segments, int requestId,
      Map<String, List<String>> segmentToEnabledInstancesMap) {
    Map<String, String> segmentToSelectedInstanceMap = new HashMap<>(HashUtil.getHashMapCapacity(segments.size()));
    for (String segment : segments) {
      List<String> enabledInstances = segmentToEnabledInstancesMap.get(segment);
      // NOTE: enabledInstances can be null when there is no enabled instances for the segment, or the instance selector
      // has not been updated (we update all components for routing in sequence)
      if (enabledInstances != null) {
        int numEnabledInstances = enabledInstances.size();
        segmentToSelectedInstanceMap.put(segment, enabledInstances.get(requestId++ % numEnabledInstances));
      }
    }
    return segmentToSelectedInstanceMap;
  }
}
