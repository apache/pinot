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
package org.apache.pinot.spi.system;

import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This factory class handles the search and creation of the {@link SystemMetadataStore} class.
 *
 * It detects all registered implementation of the SystemMetadataStore class and reflectively create
 * based on the requested configuration.
 */
public abstract class SystemMetadataStoreFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SystemMetadataStoreFactory.class);

  public static final String SYSTEM_METADATA_STORE_FACTORY_CLASS_CONFIG = "factory.class";

  protected abstract void init(PinotConfiguration configuration);

  public abstract SystemMetadataStore create();

  public static SystemMetadataStoreFactory loadFactory(PinotConfiguration configuration) {
    SystemMetadataStoreFactory systemMetadataStoreFactory;
    String systemMetadataStoreFactoryClassName = configuration.getProperty(SYSTEM_METADATA_STORE_FACTORY_CLASS_CONFIG);
    if (systemMetadataStoreFactoryClassName == null) {
      return null;
    }
    try {
      LOGGER.info("Instantiating system metadata store factory class {}", systemMetadataStoreFactoryClassName);
      systemMetadataStoreFactory = (SystemMetadataStoreFactory) Class.forName(systemMetadataStoreFactoryClassName)
          .getDeclaredConstructor().newInstance();
      LOGGER.info("Initializing system metadata store factory class {}", systemMetadataStoreFactoryClassName);
      systemMetadataStoreFactory.init(configuration);
      return systemMetadataStoreFactory;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
