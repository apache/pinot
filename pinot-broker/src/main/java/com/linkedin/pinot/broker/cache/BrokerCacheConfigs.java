/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.broker.cache;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

public class BrokerCacheConfigs {

  public static String BROKER_CACHE_CLASS = "class";
  private final Configuration _brokerCacheConfigs;

  public BrokerCacheConfigs(Configuration config) {
    _brokerCacheConfigs = config;
  }

  public String get(String brokerCacheConfigKey) {
    return _brokerCacheConfigs.getString(brokerCacheConfigKey);
  }

  public String getBrokerCacheClassName() {
    return _brokerCacheConfigs.getString(BROKER_CACHE_CLASS);
  }

  public Map<String, String> getConfigsFor(String className) {
    Map<String, String> brokerCacheClassConfigMap = new HashMap<String, String>();
    Configuration brokerCacheClassConfig = this._brokerCacheConfigs.subset(className);
    Iterator keys = brokerCacheClassConfig.getKeys();
    while (keys.hasNext()) {
      String key = (String) keys.next();
      brokerCacheClassConfigMap.put(key, brokerCacheClassConfig.getString(key));
    }
    return brokerCacheClassConfigMap;
  }

  public boolean containsKey(String key) {
    return _brokerCacheConfigs.containsKey(key);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    Iterator<String> iterators = this._brokerCacheConfigs.getKeys();
    while (iterators.hasNext()) {
      String key = iterators.next();
      sb.append("Broker Cache Props: key= " + key
          + ", value= " + this._brokerCacheConfigs.getString(key) + "\n");
    }
    return sb.toString();
  }
}
