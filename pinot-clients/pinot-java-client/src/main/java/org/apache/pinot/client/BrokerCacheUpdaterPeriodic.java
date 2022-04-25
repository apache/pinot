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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains broker cache this is updated periodically
 */
public class BrokerCacheUpdaterPeriodic implements UpdatableBrokerCache {
  private final BrokerCache _brokerCache;
  private final ScheduledExecutorService _scheduledExecutorService;
  private final long _brokerUpdateFreqInMillis;

  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerCacheUpdaterPeriodic.class);

  public BrokerCacheUpdaterPeriodic(String scheme, String controllerHost,
      int controllerPort, long brokerUpdateFreqInMillis) {
    _brokerCache = new BrokerCache(scheme, controllerHost, controllerPort);
    _scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    _brokerUpdateFreqInMillis = brokerUpdateFreqInMillis;
  }

  public void init() throws Exception {
    _brokerCache.updateBrokerData();

    if (_brokerUpdateFreqInMillis > 0) {
      _scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          try {
            _brokerCache.updateBrokerData();
          } catch (Exception e) {
            LOGGER.error("Broker cache update failed", e);
          }
        }
      }, 0, _brokerUpdateFreqInMillis, TimeUnit.MILLISECONDS);
    }
  }

  public String getBroker(String tableName) {
    return _brokerCache.getBroker(tableName);
  }

  @Override
  public List<String> getBrokers() {
    return _brokerCache.getBrokers();
  }

  @Override
  public void triggerBrokerCacheUpdate() throws Exception {
    _brokerCache.updateBrokerData();
  }

  public void close() {
    try {
      _scheduledExecutorService.shutdown();
    } catch (Exception e) {
      LOGGER.error("Cannot shutdown Broker Cache update periodic task", e);
    }
  }
}
