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
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.materializedview.metadata.PartitionFingerprint;
import org.apache.pinot.materializedview.metadata.PartitionInfo;
import org.apache.pinot.materializedview.metadata.PartitionState;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants.MaterializedViewTask;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
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

  /// Computes a [PartitionFingerprint] over the source segments overlapping
  /// `[windowStartMs, windowEndMs)`.  Single source of truth — both the scheduler (when
  /// generating tasks) and the minion executor (when validating fingerprints at commit time)
  /// must call this method, never re-implement the algorithm.
  ///
  /// Algorithm (each step is part of the byte-equality contract — changing any one of them
  /// breaks the fingerprint and forces an MV-wide recompute):
  ///
  ///   1. **Filter** to segments overlapping the half-open window: `start < windowEndMs` AND
  ///      `end >= windowStartMs`.  Asymmetric `>=` on the lower bound is intentional —
  ///      `SegmentZKMetadata#getEndTimeMs` is inclusive.
  ///   2. **Sort** the surviving list by segment name.  Listing order from
  ///      `getSegmentsZKMetadata` is not guaranteed to be stable across calls, so the sort
  ///      makes the hash deterministic.  The comparator is part of the algorithm: changing
  ///      it (e.g. to sort by CRC) is byte-equivalent to changing the hash encoding.
  ///   3. **Hash** the sorted list with `farmHashFingerprint64`, feeding each segment as
  ///      `<segmentName>\0<crc>\n`.  FarmHash64 is non-cryptographic but collision-resistant
  ///      for non-adversarial inputs; it replaces an earlier XOR-CRC scheme that exhibited
  ///      cancellation collisions (swap two segments with equal combined contribution →
  ///      identical fingerprint).
  ///
  /// Empty overlap returns [PartitionFingerprint#EMPTY] — by construction, since
  /// `farmHashFingerprint64` over zero input bytes is the constant baked into [#EMPTY].
  /// Callers MUST NOT pre-filter or pre-sort `allSegments`; the helper is responsible for
  /// both.  Pre-sorting with a different comparator would silently break byte-equality
  /// against the executor's commit-time recomputation.
  public static PartitionFingerprint computeWindowFingerprint(
      List<SegmentZKMetadata> allSegments, long windowStartMs, long windowEndMs) {
    List<SegmentZKMetadata> overlapping = new ArrayList<>();
    for (SegmentZKMetadata seg : allSegments) {
      long segStartMs = seg.getStartTimeMs();
      long segEndMs = seg.getEndTimeMs();
      if (segStartMs < windowEndMs && segEndMs >= windowStartMs) {
        overlapping.add(seg);
      }
    }
    // Empty overlap returns the shared EMPTY constant (byte-identical to hashing zero input bytes),
    // honoring the documented contract and avoiding a per-call allocation on empty windows.
    if (overlapping.isEmpty()) {
      return PartitionFingerprint.EMPTY;
    }
    overlapping.sort(Comparator.comparing(SegmentZKMetadata::getSegmentName));
    Hasher hasher = Hashing.farmHashFingerprint64().newHasher();
    for (SegmentZKMetadata seg : overlapping) {
      hasher.putString(seg.getSegmentName(), StandardCharsets.UTF_8);
      hasher.putByte((byte) 0);
      hasher.putLong(seg.getCrc());
      hasher.putByte((byte) '\n');
    }
    return new PartitionFingerprint(overlapping.size(), hasher.hash().asLong());
  }

  /// Resolves a source-table reference to its type-suffixed form (`_OFFLINE` / `_REALTIME`)
  /// using the supplied table-config lookup.  Single source of truth for the probe order —
  /// the scheduler, the minion executor, and the consistency manager all resolve through this
  /// method so the OFFLINE-first convention cannot drift between sites.
  ///
  /// A reference that already carries a type suffix is returned as-is (no probe).  Otherwise
  /// OFFLINE is probed first, then REALTIME; returns `null` when neither table config exists —
  /// callers that require a resolution fail loud with their own context-specific message.
  @Nullable
  public static String resolveTableNameWithType(Function<String, TableConfig> tableConfigProvider,
      String sourceTableName) {
    if (TableNameBuilder.getTableTypeFromTableName(sourceTableName) != null) {
      return sourceTableName;
    }
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(sourceTableName);
    if (tableConfigProvider.apply(offlineTableName) != null) {
      return offlineTableName;
    }
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(sourceTableName);
    if (tableConfigProvider.apply(realtimeTableName) != null) {
      return realtimeTableName;
    }
    return null;
  }
}
