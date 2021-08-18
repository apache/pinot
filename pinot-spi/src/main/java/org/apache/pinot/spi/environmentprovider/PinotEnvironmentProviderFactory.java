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
import com.google.common.collect.Iterables;
import java.util.List;
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

  private static final PinotEnvironmentProviderFactory INSTANCE = new PinotEnvironmentProviderFactory();

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotEnvironmentProviderFactory.class);

  private static final String CLASS = "class";
  private PinotEnvironmentProvider _pinotEnvironmentProvider;

  public static PinotEnvironmentProviderFactory getInstance() {
    return INSTANCE;
  }

  public static void init(PinotConfiguration environmentProviderFactoryConfig) {
    getInstance().initInternal(environmentProviderFactoryConfig);
  }

  public static PinotEnvironmentProvider getEnvironmentProvider(String environment) {
    return getInstance().getEnvironmentProviderInternal(environment);
  }

  private void initInternal(PinotConfiguration environmentProviderFactoryConfig) {
    // Get environment and it's respective class
    PinotConfiguration environmentConfiguration = environmentProviderFactoryConfig.subset(CLASS);
    List<String> environments = environmentConfiguration.getKeys();

    if (environments.isEmpty()) {
      LOGGER.info("Did not find any environment provider classes in the configuration");
      return;
    }

    String environment = Iterables.getOnlyElement(environments);
    String environmentProviderClassName = environmentConfiguration.getProperty(environment);
    PinotConfiguration environmentProviderConfiguration = environmentProviderFactoryConfig.subset(environment);
    LOGGER.info("Got environment {}, initializing class {}", environment, environmentProviderClassName);
    register(environment, environmentProviderClassName, environmentProviderConfiguration);
  }

  // Utility to invoke the cloud specific environment provider.
  private PinotEnvironmentProvider getEnvironmentProviderInternal(String environment) {
    Preconditions.checkState(_pinotEnvironmentProvider != null, "PinotEnvironmentProvider for environment: %s has not been initialized",
        environment);
    return _pinotEnvironmentProvider;
  }

  private void register(String environment, String environmentProviderClassName, PinotConfiguration environmentProviderConfiguration) {
    try {
      LOGGER.info("Initializing PinotEnvironmentProvider for environment {}, classname {}", environment, environmentProviderClassName);
      _pinotEnvironmentProvider = PluginManager.get().createInstance(environmentProviderClassName);
      _pinotEnvironmentProvider.init(environmentProviderConfiguration);
    } catch (Exception ex) {
      LOGGER.error("Could not instantiate environment provider for class {} with environment {}", environmentProviderClassName, environment,
          ex);
      throw new RuntimeException(ex);
    }
  }
}
