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

import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.plugin.PluginManager;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_CHANNEL_ACCESS_CONTROL_FACTORY_CLASS;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.DEFAULT_MULTI_STAGE_CHANNEL_ACCESS_CONTROL_FACTORY_CLASS;


@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class QueryAccessControlFactory {
  void init(PinotConfiguration configuration) {
  }

  abstract QueryAccessControl create();

  public static QueryAccessControlFactory fromConfig(PinotConfiguration configuration) {
    String configuredClass = configuration.getProperty(CONFIG_OF_MULTI_STAGE_CHANNEL_ACCESS_CONTROL_FACTORY_CLASS,
        DEFAULT_MULTI_STAGE_CHANNEL_ACCESS_CONTROL_FACTORY_CLASS);
    try {
      return PluginManager.get().createInstance(configuredClass);
    } catch (Exception ex) {
      return null;
    }
  }
}
