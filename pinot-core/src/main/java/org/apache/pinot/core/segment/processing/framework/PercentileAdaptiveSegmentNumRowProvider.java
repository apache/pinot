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
package org.apache.pinot.core.segment.processing.framework;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Percentile-based adaptive segment num row provider that handles heterogeneous data with
 * high variance in row sizes. Unlike {@link AdaptiveSegmentNumRowProvider} which uses a simple
 * moving average, this provider uses reservoir sampling to maintain a representative sample
 * of observed bytes-per-row values and calculates row counts based on a configurable percentile.
 *
 * <p>This is especially useful for tables where different subsets of data have vastly different
 * sizes, such as:</p>
 * <ul>
 *   <li>Multi-tenant tables where some customers have large Theta sketches and others have small ones</li>
 *   <li>Tables with variable-length text where some rows have huge documents</li>
 *   <li>Tables with optional binary fields that may or may not be populated</li>
 * </ul>
 *
 * <h3>Algorithm:</h3>
 * <ol>
 *   <li>Maintains a reservoir of up to N observed bytes-per-row values using reservoir sampling</li>
 *   <li>After each segment is created, adds the measured bytes-per-row to the reservoir</li>
 *   <li>Calculates the configured percentile (e.g., P75, P90) from the reservoir</li>
 *   <li>Uses the percentile value to calculate the next segment's row count</li>
 *   <li>Using a higher percentile (e.g., P75 or P90) biases toward creating smaller segments,
 *       which is safer for heterogeneous data</li>
 * </ol>
 *
 * <h3>Why Percentiles vs Mean:</h3>
 * <ul>
 *   <li><b>Robustness:</b> Percentiles are resistant to outliers</li>
 *   <li><b>Safety:</b> Using P75 means 75% of segments will be smaller than target, preventing oversized segments</li>
 *   <li><b>Predictability:</b> More stable estimates when data has high variance</li>
 * </ul>
 *
 * <h3>Configuration:</h3>
 * <ul>
 *   <li><b>desiredSegmentSizeBytes:</b> Target segment size in bytes (e.g., 200MB)</li>
 *   <li><b>maxNumRecordsPerSegment:</b> Hard ceiling on rows per segment</li>
 *   <li><b>percentile:</b> Which percentile to use (0-100); recommended: 75 or 90</li>
 *   <li><b>reservoirSize:</b> Number of observations to keep (default: 1000)</li>
 * </ul>
 *
 * <h3>Example Configuration:</h3>
 * <pre>
 * {
 *   "desiredSegmentSizeBytes": "209715200",  // 200MB
 *   "maxNumRecordsPerSegment": "2000000",    // 2M rows ceiling
 *   "segmentSizingStrategy": "PERCENTILE",
 *   "sizingPercentile": "75"                 // Use P75
 * }
 * </pre>
 *
 * <h3>Percentile Selection Guide:</h3>
 * <ul>
 *   <li><b>P75 (recommended):</b> Balanced approach, 75% of segments under target</li>
 *   <li><b>P90:</b> More conservative, prevents most oversized segments but may create smaller segments</li>
 *   <li><b>P50 (median):</b> Half over, half under - only use if you're okay with 50% oversized segments</li>
 * </ul>
 *
 * <h3>Thread Safety:</h3>
 * This class is NOT thread-safe. It should be used by a single thread during segment processing.
 *
 * @see AdaptiveSegmentNumRowProvider for simpler EMA-based approach suitable for homogeneous data
 */
public class PercentileAdaptiveSegmentNumRowProvider implements SegmentNumRowProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(PercentileAdaptiveSegmentNumRowProvider.class);

  // Default reservoir size (maintains last 1000 observations with uniform probability)
  private static final int DEFAULT_RESERVOIR_SIZE = 1000;

  // Default percentile (P75 = 75th percentile)
  private static final int DEFAULT_PERCENTILE = 75;

  // Default initial estimate: 5KB per row (conservative for tables with binary data)
  private static final double DEFAULT_INITIAL_BYTES_PER_ROW = 5000.0;

  // Minimum number of observations before switching from initial estimate to percentile
  private static final int MIN_OBSERVATIONS_FOR_PERCENTILE = 5;

  // Minimum rows per segment to avoid degenerate cases
  private static final int MIN_ROWS_PER_SEGMENT = 1;

  private final long _desiredSegmentSizeBytes;
  private final int _maxNumRecordsPerSegment;
  private final int _percentile;
  private final int _reservoirSize;
  private final double _initialBytesPerRowEstimate;

  private final List<Long> _reservoir;
  private final Random _random;
  private int _observationCount;
  private int _currentRowCount;
  private long _totalBytesProcessed;
  private long _totalRowsProcessed;

  /**
   * Creates a PercentileAdaptiveSegmentNumRowProvider with default parameters.
   *
   * @param desiredSegmentSizeBytes target segment size in bytes (must be positive)
   * @param maxNumRecordsPerSegment maximum rows per segment (must be positive)
   */
  public PercentileAdaptiveSegmentNumRowProvider(long desiredSegmentSizeBytes, int maxNumRecordsPerSegment) {
    this(desiredSegmentSizeBytes, maxNumRecordsPerSegment, DEFAULT_PERCENTILE, DEFAULT_RESERVOIR_SIZE,
        DEFAULT_INITIAL_BYTES_PER_ROW);
  }

  /**
   * Creates a PercentileAdaptiveSegmentNumRowProvider with custom percentile.
   *
   * @param desiredSegmentSizeBytes target segment size in bytes (must be positive)
   * @param maxNumRecordsPerSegment maximum rows per segment (must be positive)
   * @param percentile percentile to use (1-99); recommended: 75 or 90
   */
  public PercentileAdaptiveSegmentNumRowProvider(long desiredSegmentSizeBytes, int maxNumRecordsPerSegment,
      int percentile) {
    this(desiredSegmentSizeBytes, maxNumRecordsPerSegment, percentile, DEFAULT_RESERVOIR_SIZE,
        DEFAULT_INITIAL_BYTES_PER_ROW);
  }

  /**
   * Creates a PercentileAdaptiveSegmentNumRowProvider with full customization.
   *
   * @param desiredSegmentSizeBytes target segment size in bytes (must be positive)
   * @param maxNumRecordsPerSegment maximum rows per segment (must be positive)
   * @param percentile percentile to use (1-99)
   * @param reservoirSize number of observations to maintain (must be positive)
   * @param initialBytesPerRowEstimate starting estimate before sufficient observations (must be positive)
   */
  public PercentileAdaptiveSegmentNumRowProvider(long desiredSegmentSizeBytes, int maxNumRecordsPerSegment,
      int percentile, int reservoirSize, double initialBytesPerRowEstimate) {
    Preconditions.checkArgument(desiredSegmentSizeBytes > 0,
        "Desired segment size must be positive, got: %s", desiredSegmentSizeBytes);
    Preconditions.checkArgument(maxNumRecordsPerSegment > 0,
        "Max records per segment must be positive, got: %s", maxNumRecordsPerSegment);
    Preconditions.checkArgument(percentile > 0 && percentile < 100,
        "Percentile must be in (0, 100), got: %s", percentile);
    Preconditions.checkArgument(reservoirSize > 0,
        "Reservoir size must be positive, got: %s", reservoirSize);
    Preconditions.checkArgument(initialBytesPerRowEstimate > 0,
        "Initial bytes per row estimate must be positive, got: %s", initialBytesPerRowEstimate);

    _desiredSegmentSizeBytes = desiredSegmentSizeBytes;
    _maxNumRecordsPerSegment = maxNumRecordsPerSegment;
    _percentile = percentile;
    _reservoirSize = reservoirSize;
    _initialBytesPerRowEstimate = initialBytesPerRowEstimate;

    _reservoir = new ArrayList<>(_reservoirSize);
    _random = new Random();
    _observationCount = 0;
    _totalBytesProcessed = 0;
    _totalRowsProcessed = 0;
    _currentRowCount = calculateRowCount();

    LOGGER.info("Initialized PercentileAdaptiveSegmentNumRowProvider with desiredSegmentSize={}MB, "
            + "maxRecordsPerSegment={}, percentile=P{}, reservoirSize={}, initialBytesPerRow={}",
        desiredSegmentSizeBytes / (1024 * 1024), maxNumRecordsPerSegment, percentile,
        reservoirSize, initialBytesPerRowEstimate);
  }

  @Override
  public int getNumRows() {
    return _currentRowCount;
  }

  @Override
  public void updateSegmentInfo(int actualNumRows, long actualSegmentSize) {
    if (actualNumRows <= 0 || actualSegmentSize <= 0) {
      LOGGER.warn("Received invalid segment info: numRows={}, segmentSize={}. Skipping update.",
          actualNumRows, actualSegmentSize);
      return;
    }

    // Calculate bytes per row for this segment
    long bytesPerRow = actualSegmentSize / actualNumRows;

    // Update reservoir using reservoir sampling algorithm
    updateReservoir(bytesPerRow);

    // Update statistics
    _observationCount++;
    _totalBytesProcessed += actualSegmentSize;
    _totalRowsProcessed += actualNumRows;

    // Recalculate row count for next segment
    int previousRowCount = _currentRowCount;
    _currentRowCount = calculateRowCount();

    double currentPercentileValue = getPercentileBytesPerRow();

    LOGGER.info("Segment {} processed: actualRows={}, actualSize={}MB, bytesPerRow={}, "
            + "P{}={}, reservoirSize={}/{}, previousRowCount={}, newRowCount={}",
        _observationCount, actualNumRows, actualSegmentSize / (1024 * 1024),
        bytesPerRow, _percentile, String.format("%.2f", currentPercentileValue),
        _reservoir.size(), _reservoirSize, previousRowCount, _currentRowCount);
  }

  /**
   * Updates the reservoir with a new observation using the reservoir sampling algorithm.
   * This ensures uniform probability for all observations while maintaining fixed memory usage.
   *
   * @param bytesPerRow the new observation to potentially add to reservoir
   */
  private void updateReservoir(long bytesPerRow) {
    if (_reservoir.size() < _reservoirSize) {
      // Reservoir not full yet, just add
      _reservoir.add(bytesPerRow);
    } else {
      // Reservoir full, use reservoir sampling: replace a random element with probability k/n
      int randomIndex = _random.nextInt(_observationCount + 1);
      if (randomIndex < _reservoirSize) {
        _reservoir.set(randomIndex, bytesPerRow);
      }
      // Otherwise, the new observation is discarded (part of the algorithm)
    }
  }

  /**
   * Calculates the number of rows for the next segment based on the current percentile estimate.
   *
   * @return number of rows, guaranteed to be between MIN_ROWS_PER_SEGMENT and maxNumRecordsPerSegment
   */
  private int calculateRowCount() {
    double bytesPerRowEstimate = getPercentileBytesPerRow();
    int calculatedRows = (int) (_desiredSegmentSizeBytes / bytesPerRowEstimate);

    // Apply bounds: at least MIN_ROWS_PER_SEGMENT, at most maxNumRecordsPerSegment
    int boundedRows = Math.max(MIN_ROWS_PER_SEGMENT,
        Math.min(_maxNumRecordsPerSegment, calculatedRows));

    return boundedRows;
  }

  /**
   * Calculates the configured percentile from the reservoir.
   * If insufficient observations, returns the initial estimate.
   *
   * @return the percentile bytes-per-row value
   */
  private double getPercentileBytesPerRow() {
    if (_reservoir.size() < MIN_OBSERVATIONS_FOR_PERCENTILE) {
      // Not enough observations yet, use initial estimate
      return _initialBytesPerRowEstimate;
    }

    // Create a sorted copy of reservoir
    List<Long> sorted = new ArrayList<>(_reservoir);
    Collections.sort(sorted);

    // Calculate percentile index using (percentile * (N-1)) / 100
    // This gives the correct position where percentile% of values are <= result
    int index = (_percentile * (sorted.size() - 1)) / 100;

    return sorted.get(index).doubleValue();
  }

  /**
   * Gets the current percentile bytes per row estimate.
   *
   * @return current percentile estimate
   */
  public double getPercentileEstimate() {
    return getPercentileBytesPerRow();
  }

  /**
   * Gets the number of segments processed so far.
   *
   * @return segment count
   */
  public int getSegmentsProcessed() {
    return _observationCount;
  }

  /**
   * Gets the overall average bytes per row across all processed segments.
   *
   * @return average bytes per row, or 0 if no segments processed
   */
  public double getOverallAverageBytesPerRow() {
    return _totalRowsProcessed > 0 ? (double) _totalBytesProcessed / _totalRowsProcessed : 0.0;
  }

  /**
   * Gets the current reservoir size.
   *
   * @return number of observations currently in reservoir
   */
  public int getReservoirCurrentSize() {
    return _reservoir.size();
  }

  /**
   * Gets statistics about the adaptation process.
   *
   * @return human-readable statistics string
   */
  public String getStatistics() {
    return String.format(
        "PercentileAdaptiveSegmentNumRowProvider Stats: segments=%d, P%d=%.2f bytes/row, "
            + "overallAverage=%.2f bytes/row, currentRowCount=%d, reservoirSize=%d/%d, "
            + "totalRowsProcessed=%d, totalBytesProcessed=%dMB",
        _observationCount, _percentile, getPercentileBytesPerRow(), getOverallAverageBytesPerRow(),
        _currentRowCount, _reservoir.size(), _reservoirSize,
        _totalRowsProcessed, _totalBytesProcessed / (1024 * 1024));
  }

  /**
   * Gets the minimum, median, and maximum values from the reservoir for debugging.
   *
   * @return string with min/median/max statistics, or empty string if insufficient data
   */
  public String getReservoirStatistics() {
    if (_reservoir.isEmpty()) {
      return "Reservoir empty";
    }

    List<Long> sorted = new ArrayList<>(_reservoir);
    Collections.sort(sorted);

    long min = sorted.get(0);
    long median = sorted.get(sorted.size() / 2);
    long max = sorted.get(sorted.size() - 1);

    return String.format("Reservoir: min=%d, median=%d, max=%d, size=%d",
        min, median, max, sorted.size());
  }
}
