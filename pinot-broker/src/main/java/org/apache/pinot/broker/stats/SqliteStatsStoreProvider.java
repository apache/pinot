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
package org.apache.pinot.broker.stats;

import java.nio.file.Paths;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.query.planner.spi.stats.StatsStore;
import org.apache.pinot.query.planner.spi.stats.StatsStoreException;
import org.apache.pinot.query.planner.spi.stats.StatsStoreProvider;


/// Provides the default, file-backed [SqliteStatsStore].
public class SqliteStatsStoreProvider implements StatsStoreProvider {

  /// Configuration key, relative to `pinot.broker.stats.`, naming the directory that holds the
  /// database file.
  public static final String DIR_KEY = "dir";

  /// Configuration key, relative to `pinot.broker.stats.`, sizing the pool of read connections.
  /// The pool is a concurrency bound: it caps how many row estimates can be served at once.
  public static final String READ_POOL_SIZE_KEY = "readPoolSize";

  /// Configuration key, relative to `pinot.broker.stats.`, bounding how long a planner thread
  /// waits for a pooled read connection before giving up and planning without statistics.
  public static final String READ_TIMEOUT_MS_KEY = "readTimeoutMs";

  /// Upper bound on [#READ_POOL_SIZE_KEY]. Each pooled reader holds an open SQLite connection and
  /// its cached prepared statements, so an accidental extra digit should not exhaust file handles.
  private static final int MAX_READ_POOL_SIZE = 1024;

  @Override
  public String getName() {
    return "sqlite";
  }

  @Override
  public StatsStore create(Map<String, String> properties)
      throws StatsStoreException {
    String dir = properties.get(DIR_KEY);
    if (StringUtils.isBlank(dir)) {
      throw new StatsStoreException("The sqlite stats store requires a directory; none was resolved");
    }
    long rawPoolSize = positiveOrDefault(properties, READ_POOL_SIZE_KEY, SqliteStatsStore.DEFAULT_READ_POOL_SIZE);
    if (rawPoolSize > MAX_READ_POOL_SIZE) {
      throw new StatsStoreException(
          "Property '" + READ_POOL_SIZE_KEY + "' must be at most " + MAX_READ_POOL_SIZE + ", got: " + rawPoolSize);
    }
    int readPoolSize = (int) rawPoolSize;
    long readTimeoutMs =
        positiveOrDefault(properties, READ_TIMEOUT_MS_KEY, SqliteStatsStore.DEFAULT_READ_BORROW_TIMEOUT_MS);
    return new SqliteStatsStore(Paths.get(dir), readPoolSize, readTimeoutMs);
  }

  /// Reads a positive numeric property, falling back to `defaultValue` when it is absent.
  ///
  /// A present-but-unusable value is rejected rather than silently replaced by the default: it
  /// means an operator tried to tune this store and got it wrong, and a broker that quietly
  /// ignored the setting would leave them tuning a knob that does nothing.
  private static long positiveOrDefault(Map<String, String> properties, String key, long defaultValue)
      throws StatsStoreException {
    String raw = properties.get(key);
    if (StringUtils.isBlank(raw)) {
      return defaultValue;
    }
    long value;
    try {
      value = Long.parseLong(raw.trim());
    } catch (NumberFormatException e) {
      throw new StatsStoreException("Property '" + key + "' must be a number, got: " + raw);
    }
    if (value <= 0) {
      throw new StatsStoreException("Property '" + key + "' must be positive, got: " + value);
    }
    return value;
  }
}
