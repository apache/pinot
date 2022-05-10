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

package org.apache.pinot.client;

import java.util.List;


/**
 * Maintains broker cache using controller APIs
 */
public class ControllerBasedBrokerSelector implements BrokerSelector {
  private final UpdatableBrokerCache _brokerCache;

  public ControllerBasedBrokerSelector(String scheme, String controllerHost, int controllerPort,
      long brokerUpdateFreqInMillis)
      throws Exception {
    _brokerCache = new BrokerCacheUpdaterPeriodic(scheme, controllerHost, controllerPort, brokerUpdateFreqInMillis);
    _brokerCache.init();
  }

  @Override
  public String selectBroker(String table) {
    return _brokerCache.getBroker(table);
  }

  @Override
  public List<String> getBrokers() {
    return _brokerCache.getBrokers();
  }

  @Override
  public void close() {
    _brokerCache.close();
  }

  public void updateBrokers()
      throws Exception {
    _brokerCache.triggerBrokerCacheUpdate();
  }
}
