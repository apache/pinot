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
import javax.annotation.Nullable;
import org.apache.pinot.spi.utils.DataSizeUtils;


/**
 * Builds human-readable sanity-check warnings for realtime provisioning estimates.
 * <p>
 * These are advisory only: recommendation matrices are left unchanged. Thresholds are
 * named constants so they can be tuned in review without hunting through string literals.
 * See <a href="https://github.com/apache/pinot/issues/8339">#8339</a>.
 * <p>
 * Warnings are based on the maximum value across the estimate grid (not a single selected
 * host/hour pick). When {@code numHours}/{@code numHosts} axes are supplied, messages name
 * the worst cell so operators can map the warning back to the printed matrix.
 */
public final class RealtimeProvisioningWarnings {

  /**
   * Segment sizes above this often risk long consumption pauses, heap pressure during
   * segment build, and controller upload timeouts. Not a hard limit — large total data
   * volumes can still justify bigger segments after reviewing the tradeoff.
   */
  public static final long LARGE_SEGMENT_SIZE_BYTES = 500L * 1024 * 1024;

  /**
   * Many segments per host increase metadata overhead and query fan-out. Flag combinations
   * that query more than this many segments on a single host.
   */
  public static final int HIGH_SEGMENTS_PER_HOST_THRESHOLD = 5_000;

  /** Canonical docs path (current site IA). Legacy {@code operators/operating-pinot/...} redirects here. */
  public static final String REALTIME_TUNING_DOCS_URL =
      "https://docs.pinot.apache.org/operate-pinot/tuning/realtime";

  private RealtimeProvisioningWarnings() {
  }

  /**
   * Generate warnings from provisioning matrices produced by {@link MemoryEstimator}.
   * Equivalent to {@link #generate(String[][], String[][], String[][], long, int[], int[])}
   * with no axis labels.
   */
  public static List<String> generate(String[][] optimalSegmentSize, String[][] activeMemoryPerHost,
      String[][] numSegmentsQueriedPerHost, long maxUsableHostMemoryBytes) {
    return generate(optimalSegmentSize, activeMemoryPerHost, numSegmentsQueriedPerHost, maxUsableHostMemoryBytes,
        null, null);
  }

  /**
   * Generate warnings from provisioning matrices produced by {@link MemoryEstimator}.
   *
   * @param optimalSegmentSize segment size grid (human-readable sizes or {@link MemoryEstimator#NOT_APPLICABLE})
   * @param activeMemoryPerHost active/mapped memory grid (e.g. {@code 12G/24G}) or NA
   * @param numSegmentsQueriedPerHost segments-queried grid or NA
   * @param maxUsableHostMemoryBytes max usable host memory used for the estimate
   * @param numHours optional hour axis matching matrix rows (for cell labels)
   * @param numHosts optional host axis matching matrix columns (for cell labels)
   * @return ordered list of warning messages (may be empty)
   */
  public static List<String> generate(String[][] optimalSegmentSize, String[][] activeMemoryPerHost,
      String[][] numSegmentsQueriedPerHost, long maxUsableHostMemoryBytes, @Nullable int[] numHours,
      @Nullable int[] numHosts) {
    List<String> warnings = new ArrayList<>();

    CellLong maxSegment = maxParsedSizeCell(optimalSegmentSize);
    if (maxSegment._value > LARGE_SEGMENT_SIZE_BYTES) {
      warnings.add(String.format(
          "One or more estimated segment sizes in the matrix are large (up to %s at %s; threshold %s). "
              + "Very large realtime segments can cause long consumption pauses, heap pressure / OOM during "
              + "segment build, and segment upload timeouts. Review this tradeoff against total data volume "
              + "and retention; smaller segments can later be merged via realtime-to-offline or merge/rollup. "
              + "Docs: %s",
          DataSizeUtils.fromBytes(maxSegment._value), cellLabel(maxSegment, numHours, numHosts),
          DataSizeUtils.fromBytes(LARGE_SEGMENT_SIZE_BYTES), REALTIME_TUNING_DOCS_URL));
    }

    CellLong maxActiveMemory = maxActiveMemoryCell(activeMemoryPerHost);
    if (maxActiveMemory._value > maxUsableHostMemoryBytes) {
      warnings.add(String.format(
          "Estimated active memory for some host/hour combinations (up to %s at %s) exceeds max usable host memory "
              + "(%s). Those combinations may OOM or thrash; prefer more hosts, shorter retention in memory, "
              + "or a higher -maxUsableHostMemory if the hosts truly have that capacity. Docs: %s",
          DataSizeUtils.fromBytes(maxActiveMemory._value), cellLabel(maxActiveMemory, numHours, numHosts),
          DataSizeUtils.fromBytes(maxUsableHostMemoryBytes), REALTIME_TUNING_DOCS_URL));
    }

    CellLong maxSegments = maxParsedIntCell(numSegmentsQueriedPerHost);
    if (maxSegments._value > HIGH_SEGMENTS_PER_HOST_THRESHOLD) {
      warnings.add(String.format(
          "Some combinations query a very high number of segments per host (up to %d at %s; threshold %d). "
              + "High segment counts increase metadata overhead and query fan-out; consider longer "
              + "consumption hours (larger segments) or more hosts. Balancing segment size vs count is "
              + "workload-specific. Docs: %s", maxSegments._value, cellLabel(maxSegments, numHours, numHosts),
          HIGH_SEGMENTS_PER_HOST_THRESHOLD, REALTIME_TUNING_DOCS_URL));
    }

    if (containsNotApplicable(optimalSegmentSize) || containsNotApplicable(activeMemoryPerHost)
        || containsNotApplicable(numSegmentsQueriedPerHost)) {
      warnings.add(
          "Some matrix cells are NA because numHoursToConsume exceeds the retention hours considered for "
              + "queries (those combinations are not applicable). This is unrelated to host memory limits.");
    }

    return warnings;
  }

  /**
   * Format warnings for CLI / log display. Returns empty string when there are no warnings.
   */
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

  private static String cellLabel(CellLong cell, @Nullable int[] numHours, @Nullable int[] numHosts) {
    if (cell._row < 0 || cell._col < 0) {
      return "matrix";
    }
    boolean hasHours = numHours != null && cell._row < numHours.length;
    boolean hasHosts = numHosts != null && cell._col < numHosts.length;
    if (hasHours && hasHosts) {
      return String.format("numHours=%d,numHosts=%d", numHours[cell._row], numHosts[cell._col]);
    }
    if (hasHours) {
      return String.format("numHours=%d,col=%d", numHours[cell._row], cell._col);
    }
    if (hasHosts) {
      return String.format("row=%d,numHosts=%d", cell._row, numHosts[cell._col]);
    }
    return String.format("row=%d,col=%d", cell._row, cell._col);
  }

  private static CellLong maxParsedSizeCell(String[][] matrix) {
    CellLong best = CellLong.none();
    if (matrix == null) {
      return best;
    }
    for (int i = 0; i < matrix.length; i++) {
      String[] row = matrix[i];
      if (row == null) {
        continue;
      }
      for (int j = 0; j < row.length; j++) {
        Long bytes = tryParseDataSize(row[j]);
        if (bytes != null && bytes > best._value) {
          best = new CellLong(bytes, i, j);
        }
      }
    }
    return best;
  }

  private static CellLong maxActiveMemoryCell(String[][] activeMemoryPerHost) {
    CellLong best = CellLong.none();
    if (activeMemoryPerHost == null) {
      return best;
    }
    for (int i = 0; i < activeMemoryPerHost.length; i++) {
      String[] row = activeMemoryPerHost[i];
      if (row == null) {
        continue;
      }
      for (int j = 0; j < row.length; j++) {
        String cell = row[j];
        if (cell == null || cell.equals(MemoryEstimator.NOT_APPLICABLE)) {
          continue;
        }
        // Cells are "active/mapped" (e.g. "12G/24G"); compare active (first) to the host limit.
        String active = cell;
        int slash = cell.indexOf('/');
        if (slash >= 0) {
          active = cell.substring(0, slash);
        }
        Long bytes = tryParseDataSize(active);
        if (bytes != null && bytes > best._value) {
          best = new CellLong(bytes, i, j);
        }
      }
    }
    return best;
  }

  private static CellLong maxParsedIntCell(String[][] matrix) {
    CellLong best = CellLong.none();
    if (matrix == null) {
      return best;
    }
    for (int i = 0; i < matrix.length; i++) {
      String[] row = matrix[i];
      if (row == null) {
        continue;
      }
      for (int j = 0; j < row.length; j++) {
        String cell = row[j];
        if (cell == null || cell.equals(MemoryEstimator.NOT_APPLICABLE)) {
          continue;
        }
        try {
          long value = Integer.parseInt(cell.trim());
          if (value > best._value) {
            best = new CellLong(value, i, j);
          }
        } catch (NumberFormatException ignored) {
          // skip non-integer cells
        }
      }
    }
    return best;
  }

  private static boolean containsNotApplicable(String[][] matrix) {
    if (matrix == null) {
      return false;
    }
    for (String[] row : matrix) {
      if (row == null) {
        continue;
      }
      for (String cell : row) {
        if (MemoryEstimator.NOT_APPLICABLE.equals(cell)) {
          return true;
        }
      }
    }
    return false;
  }

  private static Long tryParseDataSize(String value) {
    if (value == null || value.isEmpty() || value.equals(MemoryEstimator.NOT_APPLICABLE)) {
      return null;
    }
    try {
      return DataSizeUtils.toBytes(value.trim());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /** Holds a numeric max and its matrix coordinates (row = hours index, col = hosts index). */
  private static final class CellLong {
    final long _value;
    final int _row;
    final int _col;

    CellLong(long value, int row, int col) {
      _value = value;
      _row = row;
      _col = col;
    }

    static CellLong none() {
      return new CellLong(-1L, -1, -1);
    }
  }
}
