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
package org.apache.pinot.spi.environmentprovider;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.plugin.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This factory class initializes the PinotEnvironmentProvider class.
 * It creates a PinotEnvironment object based on the URI found.
 */
public class PinotEnvironmentProviderFactory {

  private PinotEnvironmentProviderFactory() {

  }

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotEnvironmentProviderFactory.class);
  private static final String CLASS = "class";
  private static final Map<String, PinotEnvironmentProvider> PINOT_ENVIRONMENT_PROVIDER_MAP = new HashMap<>();

  public static void register(
      String environment, String environmentProviderFileName, PinotConfiguration environmentProviderConfiguration) {
    try {
      LOGGER.info("Initializing PinotEnvironmentProvider for environment {}, classname {}", environment, environmentProviderFileName);
      PinotEnvironmentProvider pinotEnvironmentProvider = PluginManager.get().createInstance(environmentProviderFileName);
      pinotEnvironmentProvider.init(environmentProviderConfiguration);
      PINOT_ENVIRONMENT_PROVIDER_MAP.put(environment, pinotEnvironmentProvider);
    } catch (Exception ex) {
      LOGGER.error(
          "Could not instantiate environment provider for class {} with environment {}", environmentProviderFileName, environment, ex);
      throw new RuntimeException(ex);
    }
  }

  public static void init(PinotConfiguration environmentProviderFactoryConfig) {
    // Get environment and their respective classes
    PinotConfiguration environmentConfiguration = environmentProviderFactoryConfig.subset(CLASS);
    List<String> environments = environmentConfiguration.getKeys();
    if (!environments.isEmpty()) {
      LOGGER.info("Did not find any environment provider classes in the configuration");
    }

    for (String environment : environments) {
      String environmentProviderClassName = environmentConfiguration.getProperty(environment);
      PinotConfiguration environmentProviderConfiguration = environmentProviderFactoryConfig.subset(environment);
      LOGGER.info("Got scheme {}, initializing class {}", environment, environmentProviderClassName);
      register(environment, environmentProviderClassName, environmentProviderConfiguration);
    }
  }

  // Utility to invoke the cloud specific environment provider.
  public static PinotEnvironmentProvider getEnvironmentProvider(String environment) {
    PinotEnvironmentProvider pinotEnvironmentProvider = PINOT_ENVIRONMENT_PROVIDER_MAP.get(environment);
    Preconditions.checkState(pinotEnvironmentProvider != null,
        "PinotEnvironmentProvider for environment: %s has not been initialized", environment);
    return pinotEnvironmentProvider;
  }
}
