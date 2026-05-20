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
package org.apache.pinot.materializedview.scheduler;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.pinot.materializedview.metadata.PartitionInfo;
import org.apache.pinot.materializedview.metadata.PartitionState;
import org.apache.pinot.spi.utils.CommonConstants.MaterializedViewTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Shared materialized-view task helpers used by the scheduler and minion executor wiring.
public final class MaterializedViewTaskUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedViewTaskUtils.class);

  private MaterializedViewTaskUtils() {
  }

  /// Reads a positive-integer cluster-config override and falls back to {@code defaultValue}
  /// when the key is unset, malformed, or non-positive. Use for caps that must be reloadable
  /// at runtime without a controller / minion restart — callers MUST invoke this on every
  /// consumer-site call rather than caching the result.
  public static int readPositiveIntClusterConfigOrDefault(
      @Nullable Function<String, String> clusterConfigLookup, String configKey, int defaultValue) {
    if (clusterConfigLookup == null) {
      return defaultValue;
    }
    String raw = clusterConfigLookup.apply(configKey);
    if (raw == null || raw.isEmpty()) {
      return defaultValue;
    }
    try {
      int parsed = Integer.parseInt(raw.trim());
      if (parsed <= 0) {
        LOGGER.warn("Cluster config '{}'='{}' is non-positive; falling back to default {}",
            configKey, raw, defaultValue);
        return defaultValue;
      }
      return parsed;
    } catch (NumberFormatException e) {
      LOGGER.warn("Cluster config '{}'='{}' is not a valid integer; falling back to default {}",
          configKey, raw, defaultValue);
      return defaultValue;
    }
  }

  /// Same as {@link #readPositiveIntClusterConfigOrDefault} for `long` values (e.g. millis).
  public static long readPositiveLongClusterConfigOrDefault(
      @Nullable Function<String, String> clusterConfigLookup, String configKey, long defaultValue) {
    if (clusterConfigLookup == null) {
      return defaultValue;
    }
    String raw = clusterConfigLookup.apply(configKey);
    if (raw == null || raw.isEmpty()) {
      return defaultValue;
    }
    try {
      long parsed = Long.parseLong(raw.trim());
      if (parsed <= 0L) {
        LOGGER.warn("Cluster config '{}'='{}' is non-positive; falling back to default {}",
            configKey, raw, defaultValue);
        return defaultValue;
      }
      return parsed;
    } catch (NumberFormatException e) {
      LOGGER.warn("Cluster config '{}'='{}' is not a valid long; falling back to default {}",
          configKey, raw, defaultValue);
      return defaultValue;
    }
  }

  /// Reads a cluster-config value via the supplied lookup; returns null if the lookup is null
  /// or returns null. Tolerates a null lookup for unit tests that don't wire a context.
  @Nullable
  public static String readClusterConfig(
      @Nullable Function<String, String> clusterConfigLookup, String configKey) {
    return clusterConfigLookup == null ? null : clusterConfigLookup.apply(configKey);
  }

  /// Returns the highest contiguous VALID upper boundary starting from `fromMs`.
  public static long computeContiguousUpperMs(long fromMs, Map<Long, PartitionInfo> partitions, long bucketMs) {
    Preconditions.checkArgument(bucketMs > 0, "bucketMs must be positive, got: %s", bucketMs);
    long cursor = fromMs;
    int maxIterations = partitions.size();
    for (int i = 0; i < maxIterations; i++) {
      PartitionInfo info = partitions.get(cursor);
      if (info == null || info.getState() != PartitionState.VALID) {
        return cursor;
      }
      cursor += bucketMs;
    }
    return cursor;
  }

  /// Parses and validates the `EFFECTIVE_LIMIT_KEY` task-config value. Throws with an actionable
  /// message when missing, malformed, or non-positive — the executor's saturation gate cannot
  /// run without a valid effective limit, so failing loud beats silent truncation.
  public static int parseEffectiveLimit(Map<String, String> configs, String tableName) {
    String limitStr = configs.get(MaterializedViewTask.EFFECTIVE_LIMIT_KEY);
    if (limitStr == null || limitStr.isEmpty()) {
      LOGGER.error("Missing {} in task config for table: {}. "
              + "Saturation gate cannot be silently skipped - upgrade the controller and retry.",
          MaterializedViewTask.EFFECTIVE_LIMIT_KEY, tableName);
      throw new IllegalStateException("Missing " + MaterializedViewTask.EFFECTIVE_LIMIT_KEY
          + " in task config for table: " + tableName);
    }
    int effectiveLimit;
    try {
      effectiveLimit = Integer.parseInt(limitStr);
    } catch (NumberFormatException e) {
      throw new IllegalStateException(
          "Invalid " + MaterializedViewTask.EFFECTIVE_LIMIT_KEY + " '" + limitStr
              + "' in task config for table: " + tableName, e);
    }
    if (effectiveLimit <= 0) {
      LOGGER.error("Non-positive effectiveLimit {} in task config for table: {}",
          effectiveLimit, tableName);
      throw new IllegalStateException(
          "effectiveLimit must be positive for table: " + tableName + ", got: " + effectiveLimit);
    }
    return effectiveLimit;
  }

  /// Throws the saturation-gate failure with an operator-actionable message.
  public static void failOnSaturation(String tableName, long windowStartMs, long windowEndMs,
      long actualRows, int effectiveLimit) {
    String message = String.format(
        "MV result saturated LIMIT: table=%s, window=[%d, %d), rows=%d, LIMIT=%d. "
            + "The materialized window is likely incomplete; failing the task to prevent "
            + "marking this partition VALID with truncated data. Narrow the time bucket / "
            + "filters in definedSQL, or add/raise the LIMIT clause in definedSQL.",
        tableName, windowStartMs, windowEndMs, actualRows, effectiveLimit);
    LOGGER.error(message);
    throw new IllegalStateException(message);
  }

  /// Fails the task if the query result set saturated the declared `LIMIT`, since that
  /// strongly suggests the window was truncated and the resulting MV would be incomplete.
  /// Delegates to [#parseEffectiveLimit] + [#failOnSaturation] so the production streaming
  /// path and the @VisibleForTesting helper share one implementation.
  public static void verifyResultNotTruncated(Map<String, String> configs, String tableName,
      long windowStartMs, long windowEndMs, int actualRows) {
    int effectiveLimit = parseEffectiveLimit(configs, tableName);
    if (actualRows >= effectiveLimit) {
      failOnSaturation(tableName, windowStartMs, windowEndMs, actualRows, effectiveLimit);
    }
  }

  /// Builds a segment name that is stable within a single attempt but unique across retries of the
  /// same window.
  public static String buildSegmentName(String tableName, long windowStartMs, long windowEndMs,
      String attemptId, int segIdx) {
    return tableName + "_" + windowStartMs + "_" + windowEndMs + "_" + attemptId + "_" + segIdx;
  }
}
