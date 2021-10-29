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

import java.util.List;


/**
 * System metadata store interface used to connect to a metadata store.
 */
public interface SystemMetadataStore {

  /**
   * Connect to a system metadata store.
   */
  void connect() throws Exception;

  /**
   * Shutdown system metadata store connection.
   */
  void shutdown()
      throws Exception;

  /**
   * Validate if this system metadata store accepts a particular {@link SystemMetadata} type.
   *
   * @param systemMetadata candidate system metadata.
   * @return true if accepts system metadata.
   */
  boolean accepts(SystemMetadata systemMetadata);

  /**
   *
   * collect a row to upload to system metadata store.
   *
   * @param systemMetadata system metadata type.
   * @param timestamp system metadata creation timestamp.
   * @param row metadata content, must match the schema provided in systemMetadata field.
   * @throws Exception
   */
  void collect(
      SystemMetadata systemMetadata,
      long timestamp,
      Object[] row) throws Exception;

  /**
   * collect multiple rows to upload to system metadata store.
   *
   * @see SystemMetadataStore#collect(SystemMetadata, long, Object[]).
   */
  default void collect(
      SystemMetadata systemMetadata,
      long timestamp,
      List<Object[]> rows) throws Exception {
    for (Object[] row : rows) {
      collect(systemMetadata, timestamp, row);
    }
  }

  /**
   * Acquire system metadata contents.
   * @param systemMetadata the system metadata type.
   * @return list of rows of the system metadata type.
   */
  List<Object[]> inspect(SystemMetadata systemMetadata)
      throws Exception;

  /**
   * Flush collected system metadata to the external store.
   *
   * @throws Exception
   */
  void flush() throws Exception;
}
