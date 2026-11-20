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
package org.apache.pinot.controller.recommender.realtime.provisioning;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.utils.DataSizeUtils;


/// Builds human-readable sanity-check warnings for realtime provisioning estimates.
///
/// These are advisory only: recommendation matrices are left unchanged. Thresholds are
/// named constants so they can be tuned in review without hunting through string literals.
/// See [#8339](https://github.com/apache/pinot/issues/8339).
///
/// Warnings name the highest estimate in the grid so operators can find that cell in the printed matrix.
public final class RealtimeProvisioningWarnings {

  /// Segment sizes above this often risk long consumption pauses, heap pressure during
  /// segment build, and controller upload timeouts. Not a hard limit - large total data
  /// volumes can still justify bigger segments after reviewing the tradeoff.
  public static final long LARGE_SEGMENT_SIZE_BYTES = 500L * 1024 * 1024;

  /// Many segments per host increase metadata overhead and query fan-out. Flag combinations
  /// that query more than this many segments on a single host.
  public static final int HIGH_SEGMENTS_PER_HOST_THRESHOLD = 5_000;

  /// Canonical docs path (current site IA). Legacy `operators/operating-pinot/...` redirects here.
  public static final String REALTIME_TUNING_DOCS_URL =
      "https://docs.pinot.apache.org/operate-pinot/tuning/realtime";

  private RealtimeProvisioningWarnings() {
  }

  /// Generate ordered warnings from exact estimates. A null cell signifies a row beyond retention.
  static List<String> generate(Estimate[][] estimates, long maxUsableHostMemoryBytes, int[] numHours, int[] numHosts) {
    Cell maxSegment = new Cell(-1, -1, -1);
    Cell maxActiveMemory = maxSegment;
    Cell maxSegments = maxSegment;
    boolean hasNotApplicable = false;
    for (int i = 0; i < estimates.length; i++) {
      for (int j = 0; j < estimates[i].length; j++) {
        Estimate estimate = estimates[i][j];
        if (estimate == null) {
          hasNotApplicable = true;
          continue;
        }
        maxSegment = maxSegment.max(estimate.segmentSizeBytes(), i, j);
        maxActiveMemory = maxActiveMemory.max(estimate.activeMemoryBytes(), i, j);
        maxSegments = maxSegments.max(estimate.segmentsPerHost(), i, j);
      }
    }
    List<String> warnings = new ArrayList<>();

    if (maxSegment.value() > LARGE_SEGMENT_SIZE_BYTES) {
      warnings.add(String.format(
          "One or more estimated segment sizes in the matrix are large (up to %s at %s; threshold %s). "
              + "Very large realtime segments can cause long consumption pauses, heap pressure / OOM during "
              + "segment build, and segment upload timeouts. Review this tradeoff against total data volume "
              + "and retention; smaller segments can later be merged via realtime-to-offline or merge/rollup. "
              + "Docs: %s",
          formatComparedSize(maxSegment.value(), LARGE_SEGMENT_SIZE_BYTES), maxSegment.label(numHours, numHosts),
          formatComparedSize(LARGE_SEGMENT_SIZE_BYTES, maxSegment.value()), REALTIME_TUNING_DOCS_URL));
    }

    if (maxActiveMemory.row() >= 0 && maxActiveMemory.value() > maxUsableHostMemoryBytes) {
      warnings.add(String.format(
          "Estimated active memory for some host/hour combinations (up to %s at %s) exceeds max usable host memory "
              + "(%s). Those combinations may OOM or thrash; prefer more hosts, shorter retention in memory, "
              + "or a higher -maxUsableHostMemory if the hosts truly have that capacity. Docs: %s",
          formatComparedSize(maxActiveMemory.value(), maxUsableHostMemoryBytes),
          maxActiveMemory.label(numHours, numHosts), formatComparedSize(maxUsableHostMemoryBytes,
              maxActiveMemory.value()), REALTIME_TUNING_DOCS_URL));
    }

    if (maxSegments.value() > HIGH_SEGMENTS_PER_HOST_THRESHOLD) {
      warnings.add(String.format(
          "Some combinations query a very high number of segments per host (up to %d at %s; threshold %d). "
              + "High segment counts increase metadata overhead and query fan-out; consider longer "
              + "consumption hours (larger segments) or more hosts. Balancing segment size vs count is "
              + "workload-specific. Docs: %s", maxSegments.value(), maxSegments.label(numHours, numHosts),
          HIGH_SEGMENTS_PER_HOST_THRESHOLD, REALTIME_TUNING_DOCS_URL));
    }

    if (hasNotApplicable) {
      warnings.add(
          "Some matrix cells are NA because numHoursToConsume exceeds the retention hours considered for "
              + "queries (those combinations are not applicable). This is unrelated to host memory limits.");
    }

    return List.copyOf(warnings);
  }

  /// Format warnings for CLI / log display. Returns empty string when there are no warnings.
  public static String formatForDisplay(List<String> warnings) {
    if (warnings == null || warnings.isEmpty()) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    sb.append("\n============================================================\n");
    sb.append("Warnings (sanity checks; matrix estimates above are unchanged):\n");
    for (int i = 0; i < warnings.size(); i++) {
      sb.append("  ").append(i + 1).append(") ").append(warnings.get(i)).append('\n');
    }
    sb.append("See ").append(REALTIME_TUNING_DOCS_URL)
        .append(" for guidance on balancing segment size, segment count, hosts, and latency.\n");
    return sb.toString();
  }

  private static String formatComparedSize(long bytes, long otherBytes) {
    String size = DataSizeUtils.fromBytes(bytes);
    return size.equals(DataSizeUtils.fromBytes(otherBytes)) ? bytes + " bytes" : size;
  }

  /// Unrounded estimates used for threshold checks.
  record Estimate(long segmentSizeBytes, long activeMemoryBytes, int segmentsPerHost) {
  }

  private record Cell(long value, int row, int col) {
    Cell max(long candidate, int candidateRow, int candidateCol) {
      return candidate > value ? new Cell(candidate, candidateRow, candidateCol) : this;
    }

    String label(int[] numHours, int[] numHosts) {
      return String.format("numHours=%d,numHosts=%d", numHours[row], numHosts[col]);
    }
  }
}
