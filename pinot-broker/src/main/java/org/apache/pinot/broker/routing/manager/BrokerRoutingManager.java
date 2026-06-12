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
package org.apache.pinot.broker.routing.manager;

import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Default broker routing manager implementation.
 *
 * <p>This class is a thin wrapper over {@link BaseBrokerRoutingManager} and exists primarily to provide the
 * canonical routing manager type for the local broker, while allowing specialized implementations
 * (e.g. remote-cluster routing managers) to extend the shared base behavior.
 */
public class BrokerRoutingManager extends BaseBrokerRoutingManager {

  public BrokerRoutingManager(BrokerMetrics brokerMetrics, ServerRoutingStatsManager serverRoutingStatsManager,
      PinotConfiguration pinotConfig) {
    super(brokerMetrics, serverRoutingStatsManager, pinotConfig);
  }
}
