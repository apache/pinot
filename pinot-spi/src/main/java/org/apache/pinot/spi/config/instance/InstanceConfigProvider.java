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
package org.apache.pinot.spi.config.instance;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * A singleton class that provides global access to instance configuration.
 * This can be used by any module to access instance-level configuration that is set during
 * instance (controller / broker / server / minion) startup.
 *
 * <p>Usage:
 * <pre>
 * // During instance startup:
 * InstanceConfigProvider.setInstanceConfig(pinotConfiguration);
 *
 * // From any module:
 * boolean value = InstanceConfigProvider.getInstanceConfig().getProperty("config.key", defaultValue);
 * </pre>
 */
public class InstanceConfigProvider {
  private static final InstanceConfigProvider INSTANCE = new InstanceConfigProvider();

  @Nullable
  private volatile PinotConfiguration _config;

  private InstanceConfigProvider() {
  }

  public static InstanceConfigProvider getInstance() {
    return INSTANCE;
  }

  /**
   * Returns the underlying PinotConfiguration, or null if not initialized.
   */
  @Nullable
  public static PinotConfiguration getInstanceConfig() {
    return INSTANCE.getConfig();
  }

  public static String getProperty(String name, String defaultValue) {
    if (getInstanceConfig() == null) {
      return defaultValue;
    }
    return getInstanceConfig().getProperty(name, defaultValue);
  }

  public static boolean getProperty(String name, boolean defaultValue) {
    if (getInstanceConfig() == null) {
      return defaultValue;
    }
    return getInstanceConfig().getProperty(name, defaultValue);
  }

  public static int getProperty(String name, int defaultValue) {
    if (getInstanceConfig() == null) {
      return defaultValue;
    }
    return getInstanceConfig().getProperty(name, defaultValue);
  }

  public static long getProperty(String name, long defaultValue) {
    if (getInstanceConfig() == null) {
      return defaultValue;
    }
    return getInstanceConfig().getProperty(name, defaultValue);
  }

  public static double getProperty(String name, double defaultValue) {
    if (getInstanceConfig() == null) {
      return defaultValue;
    }
    return getInstanceConfig().getProperty(name, defaultValue);
  }

  public static List<String> getProperty(String name, List<String> defaultValue) {
    if (getInstanceConfig() == null) {
      return defaultValue;
    }
    return getInstanceConfig().getProperty(name, defaultValue);
  }

  /**
   * Initializes the config provider with the given configuration.
   * This should be called once during instance (controller / broker / server / minion) startup.
   *
   * @param config the PinotConfiguration to use
   */
  public static void setInstanceConfig(PinotConfiguration config) {
    INSTANCE.initialize(config);
  }

  private void initialize(PinotConfiguration config) {
    _config = config;
  }

  @Nullable
  private PinotConfiguration getConfig() {
    return _config;
  }

  /**
   * Resets the config provider. This is primarily for testing purposes.
   */
  @VisibleForTesting
  public void reset() {
    _config = null;
  }
}
