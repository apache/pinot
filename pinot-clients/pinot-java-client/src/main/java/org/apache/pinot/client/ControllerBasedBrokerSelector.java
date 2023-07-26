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
import java.util.Properties;


/**
 * Maintains broker cache using controller APIs
 */
public class ControllerBasedBrokerSelector implements BrokerSelector {
  private static final String SCHEME = "scheme";

  private final UpdatableBrokerCache _brokerCache;
  private final Properties _properties;

  public ControllerBasedBrokerSelector(String scheme, String controllerHost, int controllerPort)
      throws Exception {
    this(scheme, controllerHost, controllerPort, new Properties());
  }

  public ControllerBasedBrokerSelector(String scheme, String controllerHost, int controllerPort, Properties properties)
      throws Exception {
    _properties = properties;
    String controllerUrl = controllerHost + ":" + controllerPort;
    _properties.setProperty(SCHEME, scheme);
    _brokerCache = new BrokerCacheUpdaterPeriodic(_properties, controllerUrl);
    _brokerCache.init();
  }

  public ControllerBasedBrokerSelector(Properties properties, String controllerUrl)
      throws Exception {
    _properties = properties;
    _brokerCache = new BrokerCacheUpdaterPeriodic(properties, controllerUrl);
    _brokerCache.init();
  }


  @Override
  public String selectBroker(String... tableNames) {
    return _brokerCache.getBroker(tableNames);
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
