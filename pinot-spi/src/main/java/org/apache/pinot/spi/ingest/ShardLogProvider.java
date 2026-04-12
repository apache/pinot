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
package org.apache.pinot.spi.ingest;

import java.io.Closeable;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Provider for {@link ShardLog} instances, one per table-partition combination.
 *
 * <p>The provider is initialized once during server startup and is responsible for managing the
 * lifecycle of all shard logs it creates. Implementations may back shard logs with Ratis groups,
 * Kafka topics, local files, etc.
 *
 * <p>Implementations must be thread-safe; {@link #getShardLog} may be called concurrently.
 */
public interface ShardLogProvider extends Closeable {

  /**
   * Initializes the provider with the given configuration. Called once during startup.
   *
   * @param config the Pinot configuration
   */
  void init(PinotConfiguration config);

  /**
   * Returns a shard log for the specified table and partition. Implementations should return the
   * same instance for repeated calls with the same arguments.
   *
   * @param tableNameWithType the fully qualified table name (e.g., {@code myTable_OFFLINE})
   * @param partitionId       the partition identifier
   * @return the shard log for the given table and partition
   */
  ShardLog getShardLog(String tableNameWithType, int partitionId);
}
