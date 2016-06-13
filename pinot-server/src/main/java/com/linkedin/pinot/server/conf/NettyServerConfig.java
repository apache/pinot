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
package com.linkedin.pinot.server.conf;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;


public class NettyServerConfig {

  // Netty server port
  private static String NETTY_SERVER_PORT = "port";

  private Configuration _serverNettyConfig;

  public NettyServerConfig(Configuration serverNettyConfig) throws ConfigurationException {
    _serverNettyConfig = serverNettyConfig;
    if (!_serverNettyConfig.containsKey(NETTY_SERVER_PORT)) {
      throw new ConfigurationException("Cannot find Key : " + NETTY_SERVER_PORT);
    }
  }

  /**
   * @return Netty server port
   */
  public int getPort() {
    return _serverNettyConfig.getInt(NETTY_SERVER_PORT);
  }
}
