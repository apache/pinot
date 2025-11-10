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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Adaptive segment num row provider that dynamically adjusts the number of rows per segment
 * based on actual segment sizes observed during segment generation. This provider uses an
 * Exponential Moving Average (EMA) to learn from each segment's actual size and continuously
 * refine the estimate of bytes per row.
 *
 * <p>This is particularly useful for tables with variable-sized columns (e.g., Theta sketches,
 * large text fields, or binary data) where the number of rows alone is not a reliable indicator
 * of segment size.</p>
 *
 * <h3>Algorithm:</h3>
 * <ol>
 *   <li>Starts with a conservative initial estimate of bytes per row</li>
 *   <li>After each segment is created, observes the actual segment size and row count</li>
 *   <li>Updates the bytes-per-row estimate using EMA:
 *       <code>estimate = α * measured + (1-α) * estimate</code></li>
 *   <li>Calculates the next segment's row count as:
 *       <code>desiredSegmentSize / estimatedBytesPerRow</code></li>
 *   <li>Applies the configured maximum row limit as a ceiling</li>
 * </ol>
 *
 * <h3>Configuration:</h3>
 * <ul>
 *   <li><b>desiredSegmentSizeBytes:</b> Target segment size in bytes (e.g., 200MB)</li>
 *   <li><b>maxNumRecordsPerSegment:</b> Hard ceiling on rows per segment (safety limit)</li>
 *   <li><b>learningRate:</b> EMA alpha parameter (0-1); higher = faster adaptation to changes</li>
 *   <li><b>initialBytesPerRowEstimate:</b> Conservative starting estimate</li>
 * </ul>
 *
 * <h3>Example Configuration:</h3>
 * <pre>
 * {
 *   "desiredSegmentSizeBytes": "209715200",  // 200MB
 *   "maxNumRecordsPerSegment": "2000000",    // 2M rows ceiling
 *   "segmentSizingStrategy": "ADAPTIVE"
 * }
 * </pre>
 *
 * <h3>Thread Safety:</h3>
 * This class is NOT thread-safe. It should be used by a single thread during segment processing.
 *
 * @see PercentileAdaptiveSegmentNumRowProvider for handling heterogeneous data with high variance
 */
public class AdaptiveSegmentNumRowProvider implements SegmentNumRowProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(AdaptiveSegmentNumRowProvider.class);

  // Default initial estimate: 5KB per row (conservative for tables with binary data)
  private static final double DEFAULT_INITIAL_BYTES_PER_ROW = 5000.0;

  // Default learning rate for EMA (0.3 = 30% weight to new observation, 70% to history)
  private static final double DEFAULT_LEARNING_RATE = 0.3;

  // Minimum rows per segment to avoid degenerate cases
  private static final int MIN_ROWS_PER_SEGMENT = 1;

  private final long _desiredSegmentSizeBytes;
  private final int _maxNumRecordsPerSegment;
  private final double _learningRate;

  private double _estimatedBytesPerRow;
  private int _currentRowCount;
  private int _segmentsProcessed;
  private long _totalBytesProcessed;
  private long _totalRowsProcessed;

  /**
   * Creates an AdaptiveSegmentNumRowProvider with default learning rate and initial estimate.
   *
   * @param desiredSegmentSizeBytes target segment size in bytes (must be positive)
   * @param maxNumRecordsPerSegment maximum rows per segment (must be positive)
   */
  public AdaptiveSegmentNumRowProvider(long desiredSegmentSizeBytes, int maxNumRecordsPerSegment) {
    this(desiredSegmentSizeBytes, maxNumRecordsPerSegment, DEFAULT_LEARNING_RATE, DEFAULT_INITIAL_BYTES_PER_ROW);
  }

  /**
   * Creates an AdaptiveSegmentNumRowProvider with custom parameters.
   *
   * @param desiredSegmentSizeBytes target segment size in bytes (must be positive)
   * @param maxNumRecordsPerSegment maximum rows per segment (must be positive)
   * @param learningRate EMA alpha parameter (0.0 to 1.0); higher values adapt faster
   * @param initialBytesPerRowEstimate starting estimate of bytes per row (must be positive)
   */
  public AdaptiveSegmentNumRowProvider(long desiredSegmentSizeBytes, int maxNumRecordsPerSegment,
      double learningRate, double initialBytesPerRowEstimate) {
    Preconditions.checkArgument(desiredSegmentSizeBytes > 0,
        "Desired segment size must be positive, got: %s", desiredSegmentSizeBytes);
    Preconditions.checkArgument(maxNumRecordsPerSegment > 0,
        "Max records per segment must be positive, got: %s", maxNumRecordsPerSegment);
    Preconditions.checkArgument(learningRate > 0.0 && learningRate <= 1.0,
        "Learning rate must be in (0.0, 1.0], got: %s", learningRate);
    Preconditions.checkArgument(initialBytesPerRowEstimate > 0,
        "Initial bytes per row estimate must be positive, got: %s", initialBytesPerRowEstimate);

    _desiredSegmentSizeBytes = desiredSegmentSizeBytes;
    _maxNumRecordsPerSegment = maxNumRecordsPerSegment;
    _learningRate = learningRate;
    _estimatedBytesPerRow = initialBytesPerRowEstimate;
    _currentRowCount = calculateRowCount();
    _segmentsProcessed = 0;
    _totalBytesProcessed = 0;
    _totalRowsProcessed = 0;

    LOGGER.info("Initialized AdaptiveSegmentNumRowProvider with desiredSegmentSize={}MB, "
            + "maxRecordsPerSegment={}, learningRate={}, initialBytesPerRow={}",
        desiredSegmentSizeBytes / (1024 * 1024), maxNumRecordsPerSegment, learningRate,
        initialBytesPerRowEstimate);
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

    // Calculate measured bytes per row for this segment
    double measuredBytesPerRow = (double) actualSegmentSize / actualNumRows;

    // Update estimate using Exponential Moving Average
    double previousEstimate = _estimatedBytesPerRow;
    _estimatedBytesPerRow = (_learningRate * measuredBytesPerRow)
        + ((1.0 - _learningRate) * _estimatedBytesPerRow);

    // Update statistics
    _segmentsProcessed++;
    _totalBytesProcessed += actualSegmentSize;
    _totalRowsProcessed += actualNumRows;

    // Recalculate row count for next segment
    int previousRowCount = _currentRowCount;
    _currentRowCount = calculateRowCount();

    LOGGER.info("Segment {} processed: actualRows={}, actualSize={}MB, measuredBytesPerRow={}, "
            + "previousEstimate={}, newEstimate={}, previousRowCount={}, newRowCount={}",
        _segmentsProcessed, actualNumRows, actualSegmentSize / (1024 * 1024),
        String.format("%.2f", measuredBytesPerRow), String.format("%.2f", previousEstimate),
        String.format("%.2f", _estimatedBytesPerRow), previousRowCount, _currentRowCount);
  }

  /**
   * Calculates the number of rows for the next segment based on current estimate.
   *
   * @return number of rows, guaranteed to be between MIN_ROWS_PER_SEGMENT and maxNumRecordsPerSegment
   */
  private int calculateRowCount() {
    int calculatedRows = (int) (_desiredSegmentSizeBytes / _estimatedBytesPerRow);

    // Apply bounds: at least MIN_ROWS_PER_SEGMENT, at most maxNumRecordsPerSegment
    int boundedRows = Math.max(MIN_ROWS_PER_SEGMENT,
        Math.min(_maxNumRecordsPerSegment, calculatedRows));

    return boundedRows;
  }

  /**
   * Gets the current estimated bytes per row.
   *
   * @return current estimate of bytes per row
   */
  public double getEstimatedBytesPerRow() {
    return _estimatedBytesPerRow;
  }

  /**
   * Gets the number of segments processed so far.
   *
   * @return segment count
   */
  public int getSegmentsProcessed() {
    return _segmentsProcessed;
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
   * Gets statistics about the adaptation process.
   *
   * @return human-readable statistics string
   */
  public String getStatistics() {
    return String.format(
        "AdaptiveSegmentNumRowProvider Stats: segments=%d, currentEstimate=%.2f bytes/row, "
            + "overallAverage=%.2f bytes/row, currentRowCount=%d, totalRowsProcessed=%d, totalBytesProcessed=%dMB",
        _segmentsProcessed, _estimatedBytesPerRow, getOverallAverageBytesPerRow(),
        _currentRowCount, _totalRowsProcessed, _totalBytesProcessed / (1024 * 1024));
  }
}
