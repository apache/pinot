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
package com.linkedin.pinot.broker.broker;

import org.apache.commons.configuration.Configuration;


public class BrokerClientConf {

  public static final String ENABLE_QUERY_CONSOLE = "enableConsole";
  public static final String CONSOLE_RESOURCES_PATH = "consolePath";
  public static final String QUERY_PORT = "queryPort";
  private static final int DEFAULT_QUERY_PORT = 8882;

  private Configuration config;

  public BrokerClientConf(Configuration config) {
    this.config = config;
  }

  public boolean enableConsole() {
    if (config.containsKey(ENABLE_QUERY_CONSOLE)) {
      return config.getBoolean(ENABLE_QUERY_CONSOLE);
    }
    return true;
  }

  public String getConsoleWebappPath() {
    if (config.containsKey(CONSOLE_RESOURCES_PATH)) {
      return config.getString(CONSOLE_RESOURCES_PATH);
    }
    return null;
  }

  public int getQueryPort() {
    if (config.containsKey(QUERY_PORT)) {
      return config.getInt(QUERY_PORT);
    }
    return DEFAULT_QUERY_PORT;
  }
}
