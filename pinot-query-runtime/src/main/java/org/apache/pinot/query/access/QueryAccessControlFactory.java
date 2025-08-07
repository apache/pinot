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
package org.apache.pinot.query.access;

import javax.annotation.Nullable;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.plugin.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_CHANNEL_ACCESS_CONTROL_FACTORY_CLASS;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.DEFAULT_MULTI_STAGE_CHANNEL_ACCESS_CONTROL_FACTORY_CLASS;


@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class QueryAccessControlFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryAccessControlFactory.class);
  void init(PinotConfiguration configuration) {
  }

  public abstract QueryAccessControl create();

  /**
   * Build a QueryAccessControlFactory from a `PinotConfiguration`.
   *
   * @param configuration Populated PinotConfiguration
   * @return Concrete QueryAccessControlFactory instance or null if there is an error
   */
  public static @Nullable QueryAccessControlFactory fromConfig(PinotConfiguration configuration) {
    String configuredClass = configuration.getProperty(CONFIG_OF_MULTI_STAGE_CHANNEL_ACCESS_CONTROL_FACTORY_CLASS,
        DEFAULT_MULTI_STAGE_CHANNEL_ACCESS_CONTROL_FACTORY_CLASS);
    try {
      QueryAccessControlFactory factory = PluginManager.get().createInstance(configuredClass);
      LOGGER.info("Built QueryAccessControlFactory from configured class {}", configuredClass);
      factory.init(configuration);
      return factory;
    } catch (Exception ex) {
      LOGGER.error("Caught exception attempting to load QueryAccessControlFactory {}", configuredClass, ex);
      // TODO: Consider raising a RunTime error to capture if a user's provided AccessControlFactory is not able
      //     to load. Returning null implies that we should not check access at all (failing open) but generally
      //     users who configure this value would want us to fail closed (raising an exception)
      return null;
    }
  }
}
