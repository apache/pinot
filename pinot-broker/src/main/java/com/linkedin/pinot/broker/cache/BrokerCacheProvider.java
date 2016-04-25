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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerCacheProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerCacheProvider.class);

  public static BrokerCache getBrokerCache(BrokerCacheConfigs brokerCacheConfigs) {
    String brokerCacheClazz = brokerCacheConfigs.getBrokerCacheClassName();
    try {
      Class<? extends BrokerCache> cls = (Class<? extends BrokerCache>) Class.forName(brokerCacheClazz);
      return cls.newInstance();
    } catch (Exception e) {
      LOGGER.error("Failed to initialize BrokerCache, will still continue without any broker caching!");
      return null;
    }
  }
}
