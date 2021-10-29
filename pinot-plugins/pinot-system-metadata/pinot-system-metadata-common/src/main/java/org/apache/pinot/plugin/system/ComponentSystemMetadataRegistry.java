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
package org.apache.pinot.plugin.system;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.system.SystemMetadata;
import org.apache.pinot.spi.system.SystemMetadataRegistry;
import org.apache.pinot.spi.system.SystemMetadataStore;
import org.apache.pinot.spi.system.SystemMetadataStoreFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A system metadata registry that runs as a singleton on each component (broker/controller/minion/server).
 */
public class ComponentSystemMetadataRegistry implements SystemMetadataRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(ComponentSystemMetadataRegistry.class);
  private static SystemMetadataRegistry INSTANCE = new ComponentSystemMetadataRegistry();

  private SystemMetadataStore _systemMetadataStore;
  private Map<String, SystemMetadata> _systemMetadataMap;

  private ComponentSystemMetadataRegistry() {
    _systemMetadataStore = null;
    _systemMetadataMap = new HashMap<>();
  }

  public static SystemMetadataRegistry getInstance() {
    return INSTANCE;
  }

  @Override
  public void init(PinotConfiguration pinotConfiguration) {
    PinotConfiguration pinotConfig = pinotConfiguration.subset(SYSTEM_METADATA_CONFIG_PREFIX);
    if (_systemMetadataStore == null) {
      SystemMetadataStoreFactory systemMetadataStoreFactory = SystemMetadataStoreFactory.loadFactory(pinotConfig);
      if (systemMetadataStoreFactory != null) {
        _systemMetadataStore = systemMetadataStoreFactory.create();
        try {
          _systemMetadataStore.connect();
        } catch (Exception e) {
          LOGGER.error("Unable to connect to _systemMetadataStore!", e);
          _systemMetadataStore = null;
        }
      }
    }
  }

  @Override
  public void registerSystemMetadata(SystemMetadata systemMetadata) {
    if (_systemMetadataStore == null) {
      throw new IllegalStateException("SystemMetadataStore not initialized..");
    }
    if (_systemMetadataStore.accepts(systemMetadata)) {
      _systemMetadataMap.put(systemMetadata.getSystemMetadataName(), systemMetadata);
    }
  }

  @Override
  public SystemMetadataStore getSystemMetadataStore(String metadataName) {
    return _systemMetadataMap.containsKey(metadataName) ? _systemMetadataStore : null;
  }

  @Override
  public void removeSystemMetadata(String metadataName) {
    _systemMetadataMap.remove(metadataName);
  }

  @Override
  public void shutdown()
      throws Exception {
    if (_systemMetadataStore != null) {
      _systemMetadataStore.shutdown();
    }
  }
}
