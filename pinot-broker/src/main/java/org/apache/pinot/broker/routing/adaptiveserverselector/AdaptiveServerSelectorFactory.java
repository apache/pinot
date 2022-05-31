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

import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;


public class AdaptiveServerSelectorFactory {
  private AdaptiveServerSelectorFactory() {
  }

  public static AdaptiveServerSelector getAdaptiveServerSelector(ServerRoutingStatsManager serverRoutingStatsManager,
      PinotConfiguration pinotConfig) {
    // TODO: Use brokerMetrics to intelligently select the adaptiveServerSelector type.
    String typeStr = pinotConfig.getProperty(Broker.AdaptiveServerSelector.CONFIG_OF_TYPE,
        Broker.AdaptiveServerSelector.DEFAULT_TYPE);
    Broker.AdaptiveServerSelector.Type type;
    try {
      type = Broker.AdaptiveServerSelector.Type.valueOf(typeStr.toUpperCase());
    } catch (Exception e) {
      throw new IllegalArgumentException("Illegal adaptive server selector type: " + typeStr);
    }
    switch (type) {
      case NO_OP:
        return null;
      case NUM_INFLIGHT_REQ:
        return new NumInFlightReqSelector(serverRoutingStatsManager);
      case LATENCY:
        return new MinLatencySelector(serverRoutingStatsManager);
      default:
        throw new IllegalStateException("Unsupported failure detector type: " + type);
    }
  }
}
