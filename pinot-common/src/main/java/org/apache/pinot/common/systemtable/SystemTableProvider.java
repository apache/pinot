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
package org.apache.pinot.common.systemtable;

import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


/**
 * SPI for system table providers. Implementations live with the broker path and describe virtual tables
 * (name, schema, config) and provide an {@link IndexSegment} for query execution.
 * <p>
 * Security note: system tables are intentionally not protected by database-scoped access controls (e.g. database header
 * validation is bypassed for {@code system.*} tables). Access is still governed by the configured broker
 * {@link org.apache.pinot.spi.security.AccessControl} implementation, so providers must ensure they do not expose
 * sensitive information to unauthorized users.
 */
public interface SystemTableProvider extends AutoCloseable {
  /**
   * How the broker should execute queries for this system table.
   */
  enum ExecutionMode {
    /**
     * Execute the query on the broker that received it.
     */
    BROKER_LOCAL,
    /**
     * Scatter-gather the query across all live brokers and merge results on the receiving broker.
     * <p>
     * Use this mode for system tables where each broker owns a shard of local-only data (e.g. query logs).
     */
    BROKER_SCATTER_GATHER
  }

  /**
   * Returns the {@link ExecutionMode} for this system table.
   * <p>
   * The default is {@link ExecutionMode#BROKER_LOCAL} to preserve the current behavior.
   */
  default ExecutionMode getExecutionMode() {
    return ExecutionMode.BROKER_LOCAL;
  }

  /**
   * Initializes the provider with broker-supplied dependencies.
   * <p>
   * Called once by the {@link SystemTableRegistry} after construction and before any queries are served.
   * Providers should extract whatever they need from the context and store it. The default implementation
   * is a no-op so that simple providers (e.g. those that need no external dependencies) can skip it.
   */
  default void init(SystemTableProviderContext context) {
  }

  /**
   * Logical name, e.g. "system.tables".
   */
  String getTableName();

  /**
   * Schema for the virtual table.
   */
  Schema getSchema();

  /**
   * Lightweight virtual table config.
   */
  TableConfig getTableConfig();

  /**
   * Returns an {@link IndexSegment} representing the system table contents.
   * <p>
   * The returned segment is used for query execution on the broker. Implementations may return a new segment for each
   * call (recommended) or a cached segment. Callers should invoke {@link IndexSegment#destroy()} when done with the
   * returned segment.
   * <p>
   * For {@link ExecutionMode#BROKER_SCATTER_GATHER} tables, the returned segment should represent only the local
   * broker's shard of the data; the receiving broker will scatter-gather the query across all live brokers and merge
   * the results.
   */
  IndexSegment getDataSource()
      throws Exception;

  /**
   * Shutdown hook.
   */
  @Override
  default void close()
      throws Exception {
  }
}
