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
package org.apache.pinot.core.query.scheduler.resources;

import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class to read configured resource limit policy
 */
public class ResourceLimitPolicy {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceLimitPolicy.class);

  static final int MAX_THREAD_LIMIT = Math.max(1, Runtime.getRuntime().availableProcessors());
  // The values for max threads count and pct below are educated guesses
  public static final String THREADS_PER_QUERY_PCT = "threads_per_query_pct";
  public static final int DEFAULT_THREADS_PER_QUERY_PCT = 20;
  public static final String TABLE_THREADS_SOFT_LIMIT = "table_threads_soft_limit_pct";
  public static final String TABLE_THREADS_HARD_LIMIT = "table_threads_hard_limit_pct";
  public static final int DEFAULT_TABLE_THREADS_SOFT_LIMIT = 30;
  public static final int DEFAULT_TABLE_THREADS_HARD_LIMIT = 45;

  private final int maxThreadsPerQuery;
  private final int tableThreadsSoftLimit;
  private final int tableThreadsHardLimit;

  ResourceLimitPolicy(PinotConfiguration config, int numWorkerThreads) {
    int softLimit = checkGetOrDefaultPct(config, TABLE_THREADS_SOFT_LIMIT, DEFAULT_TABLE_THREADS_SOFT_LIMIT);
    tableThreadsSoftLimit = Math.min(numWorkerThreads, Math.max(1, numWorkerThreads * softLimit / 100));
    int hardLimit = checkGetOrDefaultPct(config, TABLE_THREADS_HARD_LIMIT, DEFAULT_TABLE_THREADS_HARD_LIMIT);
    // hardLimit <= tableThreadsHardLimit < numWorkerThreads
    tableThreadsHardLimit =
        Math.min(numWorkerThreads, Math.max(tableThreadsSoftLimit, numWorkerThreads * hardLimit / 100));

    int tpqPct = checkGetOrDefaultPct(config, THREADS_PER_QUERY_PCT, DEFAULT_THREADS_PER_QUERY_PCT);
    // 1 <= maxThreadsPerQuery <= tableThreadsHardLimit
    maxThreadsPerQuery =
        Math.min(tableThreadsHardLimit, Math.min(MAX_THREAD_LIMIT, Math.max(1, numWorkerThreads * tpqPct / 100)));

    LOGGER.info("MaxThreadsPerQuery: {}, tableThreadsSoftLimit: {}, tableThreadsHardLimit: {}", maxThreadsPerQuery,
        tableThreadsSoftLimit, tableThreadsHardLimit);
  }

  private int checkGetOrDefaultPct(PinotConfiguration schedulerConfig, String key, int defaultValue) {
    int pct = schedulerConfig.getProperty(key, defaultValue);
    if (pct <= 0 || pct > 100) {
      LOGGER.error("Incorrect value for {}, value: {}; using default: {}", key, pct, defaultValue);
      pct = defaultValue;
    }
    return pct;
  }

  int getMaxThreadsPerQuery() {
    return maxThreadsPerQuery;
  }

  int getTableThreadsSoftLimit() {
    return tableThreadsSoftLimit;
  }

  int getTableThreadsHardLimit() {
    return tableThreadsHardLimit;
  }
}
