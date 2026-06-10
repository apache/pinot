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
package org.apache.pinot.query.planner.spi.stats;

import java.util.OptionalLong;
import javax.annotation.Nullable;


/**
 * No-op implementation of {@link PinotStatisticsProvider} that returns {@code null} or empty
 * for all queries.
 *
 * <p>Used as a safe default when no statistics back-end is configured, allowing the planner to
 * fall back to heuristic cost estimation.
 *
 * <p>Thread-safety: stateless singleton; unconditionally thread-safe.
 */
public final class NoOpStatisticsProvider implements PinotStatisticsProvider {

  /** Singleton instance. */
  public static final NoOpStatisticsProvider INSTANCE = new NoOpStatisticsProvider();

  private NoOpStatisticsProvider() {
  }

  @Nullable
  @Override
  public TableStatistics getTableStatistics(String tableName) {
    return null;
  }

  @Nullable
  @Override
  public ColumnStatistics getColumnStatistics(String tableName, String columnName) {
    return null;
  }

  @Override
  public OptionalLong estimateRowsInTimeRange(String tableName, long startMs, long endMs) {
    return OptionalLong.empty();
  }
}
