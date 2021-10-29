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


/**
 * Registry for metadata.
 *
 * It is used to keep track of the metadata reporting, such as:
 *   1. what metadata is being reported by Pinot and which connector is used to store the
 * metadata
 *   2. validate if metadata schema is compatible with the connector/table requirements.
 *   3. registers connector handler callback for each metadata report.
 */
public interface SystemMetadataRegistry {
  String SYSTEM_METADATA_CONFIG_PREFIX = "system.metadata";

  /**
   * Initialize metadata store with configurations.
   *
   * @param pinotConfiguration
   * @return system metadata store object.
   */
  void init(PinotConfiguration pinotConfiguration);

  /**
   * Register system metadata.
   * @param systemMetadata
   */
  void registerSystemMetadata(SystemMetadata systemMetadata);

  /**
   * Acquire the system metadata store that this system metadata type is registered under.
   *
   * @param metadataName the name of the metadata.
   * @return the system metadata store instance use to store this metadata type.
   */
  SystemMetadataStore getSystemMetadataStore(String metadataName);

  /**
   * Remove metadata by its name.
   */
  void removeSystemMetadata(String metadataName);

  /**
   * shutdown metadata
   */
  void shutdown() throws Exception;
}
