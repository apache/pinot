
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
package com.linkedin.pinot.controller.helix.core.rebalance;

import java.util.HashMap;
import java.util.Map;


/**
 * Custom user config provided in rebalance, which will vary according to rebalance strategy
 */
public class RebalanceUserConfig {

  private Map<String, String> _configs = new HashMap<>(1);

  public String getConfig(String key) {
    return _configs.get(key);
  }

  public String getConfig(String key, String defaultValue) {
    String value = _configs.get(key);
    if (value == null) {
      value = defaultValue;
    }
    return value;
  }

  public void addConfig(String key, String value) {
    _configs.put(key, value);
  }

  public void addAllConfigs(Map<String, String> configs) {
    if (configs != null) {
      for (Map.Entry<String, String> entry : configs.entrySet()) {
        _configs.put(entry.getKey(), entry.getValue());
      }
    }
  }
}
