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
package org.apache.pinot.segment.local.customobject;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.pinot.segment.local.utils.TDigestUtils;
import org.apache.pinot.segment.local.utils.TDigestUtils.SerializedTDigestMetadata;
import org.apache.pinot.segment.spi.customobject.TDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Accumulates raw values and serialized TDigests into primitive centroids using Pinot's K1 weight-limit rule.
///
/// Raw values are sorted in small batches; serialized centroids retain their existing sorted order. Both are
/// linearly merged with accumulated centroids. K1 preserves Pinot's existing middle-quantile behavior, and the
/// legacy verbose and small encodings preserve stored data and mixed-version wire compatibility.
///
/// Serialized state keeps its first digest pending and allocates centroid buffers only when another input must be
/// merged or the digest is queried. Raw-value buffers remain unallocated until a raw value is added.
///
/// Historical corrupted payloads remain opaque and byte-exact, with NaN statistics. Their reported historical mass
/// may be invalid; they cannot be mutated or mixed with another distribution. Such an operation fails before
/// changing aggregate mass, because neither invented statistics nor synthetic NaN bytes are safe for old readers.
///
/// Instances are externally serialized per aggregation or group key and are not safe for concurrent mutation.
public final class PercentileTDigestAccumulator extends TDigest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PercentileTDigestAccumulator.class);
  private static final int MIN_RAW_BUFFER_SIZE = 256;
  private static final int MAX_RAW_BUFFER_SIZE = 10_000;
  private static final int MIN_COMPRESSION = 10;
  private static final int TWO_LEVEL_COMPRESSION_MULTIPLIER = 2;
  private static final int MIN_INCREMENTAL_COMPRESSION = 50;

  private final double _compression;
  private final boolean _useTwoLevelRawCompression;
  private final double _workingCompression;
  private final double _publicMaxWeightScale;
  private final double _workingMaxWeightScale;
  private final int _centroidCapacity;
  private final int _pendingCentroidCapacity;
  private double[] _rawValues;
  private double[] _centroidMeans;
  private double[] _centroidWeights;
  private double[] _outputMeans;
  private double[] _outputWeights;
  private double[] _incomingMeans;
  private double[] _incomingWeights;
  private double[] _sortedIncomingMeans;
  private double[] _sortedIncomingWeights;
  private SerializedTDigestInput _serializedTDigestInput;
  private byte[] _pendingSerializedTDigest;
  private byte[] _originalFractionalBytes;
  private SerializedTDigestMetadata _pendingSerializedMetadata;
  private boolean _legacyDegraded;
  private byte[] _serializedBytesForWrite;
  private boolean _hasFractionalWeights;

  private int _numRawValues;
  private int _numCentroids;
  private int _numIncomingCentroids;
  private int _serializedMainCapacity;
  private int _serializedBufferCapacity;
  private int _mergeCount;
  private boolean _publiclyCompressed;
  private boolean _incomingCentroidsSorted = true;
  private double _totalWeight;
  private double _incomingWeight;
  private double _min = Double.POSITIVE_INFINITY;
  private double _max = Double.NEGATIVE_INFINITY;

  public PercentileTDigestAccumulator(int compression) {
    this(positiveCompression(compression), true, true);
  }

  public PercentileTDigestAccumulator(double compression) {
    this(compression, true, true);
  }

  private PercentileTDigestAccumulator(double compression, boolean allocateRawBuffer, boolean allocateMergeBuffers) {
    this(compression, allocateRawBuffer, allocateMergeBuffers, false);
  }

  private PercentileTDigestAccumulator(double compression, boolean allocateRawBuffer, boolean allocateMergeBuffers,
      boolean useTwoLevelRawCompression) {
    TDigestUtils.validateCompression(compression);
    _useTwoLevelRawCompression = useTwoLevelRawCompression;
    _compression = Math.max(MIN_COMPRESSION, compression);
    _workingCompression = TWO_LEVEL_COMPRESSION_MULTIPLIER * _compression;
    _publicMaxWeightScale = calculateK1MaxWeightScale(_compression);
    _workingMaxWeightScale = calculateK1MaxWeightScale(_workingCompression);
    long roundedCompression = (long) Math.ceil(_compression);
    _centroidCapacity = TDigestUtils.getDefaultCentroidCapacity(_compression);
    _pendingCentroidCapacity = getPendingCentroidCapacity(_centroidCapacity);
    if (allocateRawBuffer) {
      _rawValues = new double[getRawBufferSize(roundedCompression)];
    }
    if (allocateMergeBuffers) {
      int initialCapacity = Math.min(_centroidCapacity, MIN_RAW_BUFFER_SIZE);
      _centroidMeans = new double[initialCapacity];
      _centroidWeights = new double[initialCapacity];
      _outputMeans = new double[initialCapacity];
      _outputWeights = new double[initialCapacity];
    }
  }

  /// Creates a lazy digest with the legacy two-level compression used by Star-tree and general digest aggregation.
  public static PercentileTDigestAccumulator forLegacyAggregation(double compression) {
    return new PercentileTDigestAccumulator(compression, false, false, true);
  }

  public static PercentileTDigestAccumulator forSerializedTDigest(byte[] bytes) {
    return new PercentileTDigestAccumulator(readCompression(bytes), false, false);
  }

  public static PercentileTDigestAccumulator forSerializedTDigest(SerializedTDigestInput input) {
    return new PercentileTDigestAccumulator(input._compression, false, false);
  }

  public static PercentileTDigestAccumulator forSerializedTDigestWithMergeBuffers(byte[] bytes) {
    return new PercentileTDigestAccumulator(readCompression(bytes), false, true);
  }

  public static PercentileTDigestAccumulator forSerializedTDigest(ByteBuffer input) {
    SerializedTDigestInput serialized = new SerializedTDigestInput();
    serialized.reset(input);
    PercentileTDigestAccumulator accumulator = forSerializedTDigest(serialized);
    accumulator.addSerializedTDigest(serialized);
    return accumulator;
  }

  public static PercentileTDigestAccumulator forReduction(double compression) {
    return new PercentileTDigestAccumulator(positiveCompression(compression), false, false);
  }

  /// Copies primitive centroids without encoding and decoding them. Zero weights are ignored, and unordered
  /// means are sorted when initializing a stored, already compressed distribution; caller arrays are not mutated.
  /// Subsequent inputs use the normal buffered merge. Positive weights and consistent extrema are required.
  public void addCentroids(double[] means, double[] weights, int count, double min, double max,
      boolean alreadyCompressed) {
    requireMutable();
    if (count < 0 || count > means.length || count > weights.length) {
      throw new IllegalArgumentException("Invalid TDigest centroid count: " + count);
    }
    double totalWeight = 0.0;
    int positiveCount = 0;
    boolean sorted = true;
    boolean fractionalWeights = false;
    double previousMean = Double.NEGATIVE_INFINITY;
    for (int i = 0; i < count; i++) {
      double weight = weights[i];
      if (weight == 0.0) {
        continue;
      }
      double mean = means[i];
      if (!(weight > 0.0) || !Double.isFinite(weight) || !(mean >= min && mean <= max)) {
        throw new IllegalArgumentException("Invalid TDigest centroid");
      }
      totalWeight += weight;
      sorted &= mean >= previousMean;
      previousMean = mean;
      fractionalWeights |= weight != Math.rint(weight);
      positiveCount++;
    }
    checkTotalWeight(getTotalWeight() + totalWeight);
    if (positiveCount == 0) {
      return;
    }
    materializePendingSerializedTDigest();
    _originalFractionalBytes = null;
    _hasFractionalWeights |= fractionalWeights;
    if (hasNoInputs() && alreadyCompressed) {
      ensureCentroidCapacity(Math.addExact(positiveCount, 2));
      for (int i = 0; i < count; i++) {
        if (weights[i] > 0.0) {
          _centroidMeans[_numCentroids] = means[i];
          _centroidWeights[_numCentroids++] = weights[i];
        }
      }
      if (!sorted) {
        it.unimi.dsi.fastutil.Arrays.mergeSort(0, _numCentroids,
            (first, second) -> Double.compare(_centroidMeans[first], _centroidMeans[second]),
            (first, second) -> {
              double mean = _centroidMeans[first];
              _centroidMeans[first] = _centroidMeans[second];
              _centroidMeans[second] = mean;
              double weight = _centroidWeights[first];
              _centroidWeights[first] = _centroidWeights[second];
              _centroidWeights[second] = weight;
            });
      }
      _totalWeight = totalWeight;
      _min = min;
      _max = max;
      normalizeBoundaryCentroids();
      _publiclyCompressed = true;
    } else {
      for (int i = 0; i < count; i++) {
        if (weights[i] > 0.0) {
          bufferIncomingCentroid(means[i], weights[i]);
        }
      }
      _min = Math.min(_min, min);
      _max = Math.max(_max, max);
    }
  }

  public void add(double[] values, int from, int toExclusive) {
    if (from >= toExclusive) {
      return;
    }
    requireMutable();
    materializePendingSerializedTDigest();
    ensureRawBuffer();
    while (from < toExclusive) {
      if (_numRawValues == _rawValues.length
          || _numRawValues + _numIncomingCentroids == getPendingInputLimit()) {
        flush();
      }
      int numValues = Math.min(toExclusive - from, Math.min(_rawValues.length - _numRawValues,
          getPendingInputLimit() - _numRawValues - _numIncomingCentroids));
      int rawOffset = _numRawValues;
      for (int i = 0; i < numValues; i++) {
        double value = values[from + i];
        if (Double.isNaN(value)) {
          throw new IllegalArgumentException("Cannot add NaN to t-digest");
        }
        _rawValues[rawOffset + i] = value;
      }
      _originalFractionalBytes = null;
      from += numValues;
      _numRawValues += numValues;
      if (_numRawValues == _rawValues.length
          || _numRawValues + _numIncomingCentroids == getPendingInputLimit()) {
        flush();
      }
    }
  }

  @Override
  public void add(double value) {
    if (Double.isNaN(value)) {
      throw new IllegalArgumentException("Cannot add NaN to t-digest");
    }
    requireMutable();
    materializePendingSerializedTDigest();
    ensureRawBuffer();
    if (_numRawValues == _rawValues.length
        || _numRawValues + _numIncomingCentroids == getPendingInputLimit()) {
      flush();
    }
    _originalFractionalBytes = null;
    _rawValues[_numRawValues++] = value;
  }

  @Override
  public void add(double value, double weight) {
    if (weight == 1) {
      add(value);
      return;
    }
    if (Double.isNaN(value)) {
      throw new IllegalArgumentException("Cannot add NaN to t-digest");
    }
    if (!(weight > 0.0) || !Double.isFinite(weight)) {
      throw new IllegalArgumentException("TDigest weight must be positive: " + weight);
    }
    requireMutable();
    materializePendingSerializedTDigest();
    bufferIncomingCentroid(value, weight);
    _originalFractionalBytes = null;
    _hasFractionalWeights |= weight != Math.rint(weight);
    _min = Math.min(_min, value);
    _max = Math.max(_max, value);
  }

  @Override
  public void add(TDigest other) {
    requireMutable();
    if (other instanceof PercentileTDigestAccumulator) {
      addAccumulator((PercentileTDigestAccumulator) other);
      return;
    }
    if (!other.hasValidStatistics()) {
      materializePendingSerializedTDigest();
      SerializedTDigestInput input = getSerializedTDigestInput();
      input.reset(ByteBuffer.wrap(TDigestUtils.serialize(other)), false);
      addSerializedTDigest(input);
      return;
    }
    materializePendingSerializedTDigest();
    double otherWeight = other.getTotalWeight();
    checkTotalWeight(_totalWeight + _incomingWeight + _numRawValues + otherWeight);
    for (Centroid centroid : other.centroids()) {
      double weight = centroid.weight();
      if (weight == 0.0) {
        continue;
      }
      if (!(weight > 0.0) || !Double.isFinite(weight) || Double.isNaN(centroid.mean())) {
        throw new IllegalArgumentException("Invalid TDigest centroid");
      }
      _hasFractionalWeights |= weight != Math.rint(weight);
      bufferIncomingCentroid(centroid.mean(), weight);
      _originalFractionalBytes = null;
    }
    if (otherWeight != 0.0) {
      _min = Math.min(_min, other.getMin());
      _max = Math.max(_max, other.getMax());
    }
  }

  @Override
  public void add(List<? extends TDigest> others) {
    for (TDigest other : others) {
      add(other);
    }
  }

  private void addAccumulator(PercentileTDigestAccumulator other) {
    if (other == this) {
      compress();
      if (_numCentroids == 0) {
        return;
      }
      // Snapshot only self-merges; fresh fractional mass need not be representable by a legacy wire payload.
      addCentroids(Arrays.copyOf(_centroidMeans, _numCentroids), Arrays.copyOf(_centroidWeights, _numCentroids),
          _numCentroids, _min, _max, false);
      return;
    }
    if (other._originalFractionalBytes != null && _compression == other._compression && getTotalWeight() == 0.0) {
      materializePendingSerializedTDigest();
      SerializedTDigestInput input = getSerializedTDigestInput();
      input.reset(other._originalFractionalBytes);
      input._retainedBytes = other._originalFractionalBytes;
      addSerializedTDigest(input);
      return;
    }
    if (other._pendingSerializedTDigest != null) {
      // Materialize before resetting the reusable reader: materialization uses that same reader.
      materializePendingSerializedTDigest();
      SerializedTDigestInput input = getSerializedTDigestInput();
      input.reset(other._pendingSerializedTDigest, other._pendingSerializedMetadata);
      input._retainedBytes = other._pendingSerializedTDigest;
      addSerializedTDigest(input);
      return;
    }
    if (other._legacyDegraded) {
      addSerializedTDigest(other.serialize());
      return;
    }

    // MergingDigest compressed merge sources in place. Aggregation owns these mutable inputs exclusively.
    other.compress();
    materializePendingSerializedTDigest();
    checkTotalWeight(_totalWeight + _incomingWeight + _numRawValues + other._totalWeight);
    _hasFractionalWeights |= other._hasFractionalWeights;
    preserveSerializedCapacity(other._serializedMainCapacity, other._serializedBufferCapacity);
    bufferIncomingCentroids(other._centroidMeans, other._centroidWeights, other._numCentroids);
    if (other._totalWeight > 0.0) {
      _originalFractionalBytes = null;
    }
    _min = Math.min(_min, other._min);
    _max = Math.max(_max, other._max);
  }

  public void addSerializedTDigest(byte[] bytes) {
    requireMutable();
    materializePendingSerializedTDigest();
    SerializedTDigestInput input = getSerializedTDigestInput();
    input.reset(bytes);
    addSerializedTDigest(input);
  }

  private SerializedTDigestInput getSerializedTDigestInput() {
    if (_serializedTDigestInput == null) {
      _serializedTDigestInput = new SerializedTDigestInput();
    }
    return _serializedTDigestInput;
  }

  public void addSerializedTDigest(SerializedTDigestInput input) {
    requireMutable();
    if (prepareSerializedTDigest(input)) {
      mergeSerializedTDigest(input, false);
    }
  }

  public void addSerializedTDigestDirect(SerializedTDigestInput input) {
    requireMutable();
    if (prepareSerializedTDigest(input)) {
      mergeSerializedTDigest(input, true);
    }
  }

  private boolean prepareSerializedTDigest(SerializedTDigestInput input) {
    if (_pendingSerializedTDigest == null && hasNoInputs()) {
      input.inspectMetadata();
      _hasFractionalWeights |= input._metadata.fractionalWeights();
      _pendingSerializedTDigest = input.retainBytes();
      _pendingSerializedMetadata = input._metadata;
      if (input.hasUnrepresentableBoundaryMass() && _compression == input._compression) {
        // Alias the retained snapshot; historical fractional bytes remain byte-exact after read-only materialization.
        _originalFractionalBytes = _pendingSerializedTDigest;
      }
      if (input._metadata.needsLegacyFallback()) {
        markLegacyDegraded(input);
      }
      return false;
    }
    materializePendingSerializedTDigest();
    input.decode();
    if (input._metadata.needsLegacyFallback()) {
      if (hasNoInputs()) {
        _pendingSerializedTDigest = input.retainBytes();
        _pendingSerializedMetadata = input._metadata;
        markLegacyDegraded(input);
        return false;
      }
      throw corruptedMutation();
    }
    checkTotalWeight(_totalWeight + _incomingWeight + _numRawValues + input._totalWeight);
    if (input._totalWeight > 0.0) {
      _originalFractionalBytes = null;
    }
    _hasFractionalWeights |= input._metadata.fractionalWeights();
    return true;
  }

  private boolean hasNoInputs() {
    return _numCentroids == 0 && _totalWeight == 0.0 && _numRawValues == 0 && _numIncomingCentroids == 0;
  }

  private void mergeSerializedTDigest(SerializedTDigestInput input, boolean direct) {
    if (input._totalWeight == 0.0) {
      return;
    }
    preserveSerializedCapacity(input._mainCapacity, input._bufferCapacity);
    if (hasNoInputs()) {
      if (input._totalWeight != 0.0) {
        ensureCentroidCapacity(input._numCentroids);
        System.arraycopy(input._means, 0, _centroidMeans, 0, input._numCentroids);
        System.arraycopy(input._weights, 0, _centroidWeights, 0, input._numCentroids);
        _numCentroids = input._numCentroids;
        _totalWeight = input._totalWeight;
        _min = input._min;
        _max = input._max;
        // Stored centroids already went through the writer's public compression. Reading or repairing their
        // endpoints needs another K1 pass only when the output compression differs from the stored compression.
        _publiclyCompressed = _compression == input._compression;
      }
      return;
    }
    if (direct) {
      flush();
      boolean runBackwards = (_mergeCount++ & 1) != 0;
      if (!mergeSorted(input._means, input._weights, input._numCentroids, input._totalWeight,
          getIncrementalCompression(), runBackwards)) {
        return;
      }
    } else {
      bufferIncomingCentroids(input._means, input._weights, input._numCentroids);
    }
    _min = Math.min(_min, input._min);
    _max = Math.max(_max, input._max);
  }

  @Override
  public void compress() {
    if (_legacyDegraded) {
      return;
    }
    if (_pendingSerializedTDigest != null || _numRawValues > 0 || _numIncomingCentroids > 0
        || !_publiclyCompressed && _numCentroids > 0) {
      _serializedBytesForWrite = null;
    }
    materializePendingSerializedTDigest();
    if (_numRawValues > 0 || _numIncomingCentroids > 0) {
      // MergingDigest 3.3 combines pending values and existing centroids directly at public compression. Flushing at
      // working compression first would add an extra lossy merge pass and materially increase reducer rank error.
      flush(_compression);
      _publiclyCompressed = true;
    } else if (!_publiclyCompressed && _numCentroids > 0) {
      recompressCentroids(_compression);
      _publiclyCompressed = true;
    }
  }

  @Override
  public long size() {
    return (long) getTotalWeight();
  }

  @Override
  public boolean hasValidStatistics() {
    return !_legacyDegraded;
  }

  @Override
  public double getTotalWeight() {
    return _pendingSerializedTDigest != null ? _pendingSerializedMetadata.totalWeight()
        : _totalWeight + _incomingWeight + _numRawValues;
  }

  /// Bounds serialized output centroids from buffered inputs, including up to two boundary splits before a merge
  /// and two more before writing, without flushing or using the potentially huge input mass.
  public long getCentroidCountUpperBound() {
    return _pendingSerializedTDigest != null ? (long) _pendingSerializedMetadata.centroidCount() + 4L
        : (long) _numCentroids + _numRawValues + _numIncomingCentroids + 4L;
  }

  /// Whether fractional input weights invalidate the usual bound of one centroid per unit of mass.
  public boolean hasFractionalWeights() {
    return _hasFractionalWeights;
  }

  /// Whether unchanged historical fractional boundaries still require their original byte representation.
  public boolean hasOriginalFractionalPayload() {
    return _originalFractionalBytes != null;
  }

  @Override
  public double cdf(double value) {
    if (Double.isNaN(value) || Double.isInfinite(value)) {
      throw new IllegalArgumentException(String.format("Invalid value: %f", value));
    }
    if (_legacyDegraded) {
      return Double.NaN;
    }
    materializePendingSerializedTDigest();
    flush();
    if (_numCentroids == 0) {
      return Double.NaN;
    }
    if (_numCentroids == 1) {
      double width = _max - _min;
      if (value < _min) {
        return 0.0;
      } else if (value > _max) {
        return 1.0;
      } else if (value - _min <= width) {
        return 0.5;
      } else {
        return (value - _min) / width;
      }
    }
    if (value < _min) {
      return 0.0;
    }
    if (value > _max) {
      return 1.0;
    }
    if (value < _centroidMeans[0]) {
      double width = _centroidMeans[0] - _min;
      if (width > 0.0) {
        if (_centroidWeights[0] < 1.0) {
          return (_centroidWeights[0] / 2.0 * (value - _min) / width) / _totalWeight;
        }
        return value == _min ? 0.5 / _totalWeight
            : (1.0 + (value - _min) / width * (_centroidWeights[0] / 2.0 - 1.0)) / _totalWeight;
      }
      return 0.0;
    }
    int lastIndex = _numCentroids - 1;
    if (value > _centroidMeans[lastIndex]) {
      double width = _max - _centroidMeans[lastIndex];
      if (width > 0.0) {
        if (_centroidWeights[lastIndex] < 1.0) {
          return 1.0 - (_centroidWeights[lastIndex] / 2.0 * (_max - value) / width) / _totalWeight;
        }
        return value == _max ? 1.0 - 0.5 / _totalWeight
            : 1.0 - (1.0 + (_max - value) / width * (_centroidWeights[lastIndex] / 2.0 - 1.0))
                / _totalWeight;
      }
      return 1.0;
    }

    double weightSoFar = 0.0;
    for (int i = 0; i < lastIndex; i++) {
      if (_centroidMeans[i] == value) {
        double equalWeight = 0.0;
        int equalIndex = i;
        while (equalIndex < _numCentroids && _centroidMeans[equalIndex] == value) {
          equalWeight += _centroidWeights[equalIndex];
          equalIndex++;
        }
        return (weightSoFar + equalWeight / 2.0) / _totalWeight;
      }
      if (_centroidMeans[i] <= value && _centroidMeans[i + 1] > value) {
        if (!Double.isFinite(_centroidMeans[i]) || !Double.isFinite(_centroidMeans[i + 1])) {
          return (weightSoFar + _centroidWeights[i]) / _totalWeight;
        }
        double width = _centroidMeans[i + 1] - _centroidMeans[i];
        if (width > 0.0) {
          double leftExcludedWeight = getPointMassHalfWeight(i);
          double rightExcludedWeight = getPointMassHalfWeight(i + 1);
          double halfWeight = (_centroidWeights[i] + _centroidWeights[i + 1]) / 2.0;
          double interpolatedWeight = halfWeight - leftExcludedWeight - rightExcludedWeight;
          double base = weightSoFar + _centroidWeights[i] / 2.0 + leftExcludedWeight;
          return (base + interpolatedWeight * (value - _centroidMeans[i]) / width) / _totalWeight;
        }
        return (weightSoFar + (_centroidWeights[i] + _centroidWeights[i + 1]) / 2.0) / _totalWeight;
      }
      weightSoFar += _centroidWeights[i];
    }
    if (value == _centroidMeans[lastIndex]) {
      return 1.0 - Math.min(1.0, _centroidWeights[lastIndex]) / 2.0 / _totalWeight;
    }
    throw new IllegalStateException("Unable to compute TDigest CDF");
  }

  @Override
  public double quantile(double quantile) {
    if (Double.isNaN(quantile) || quantile < 0.0 || quantile > 1.0) {
      throw new IllegalArgumentException("q should be in [0,1], got " + quantile);
    }
    if (_legacyDegraded) {
      return Double.NaN;
    }
    materializePendingSerializedTDigest();
    flush();
    if (_numCentroids == 0) {
      return Double.NaN;
    }
    if (quantile == 0.0) {
      return _min;
    }
    if (quantile == 1.0) {
      return _max;
    }
    if (_numCentroids == 1) {
      return _centroidMeans[0];
    }

    double index = quantile * _totalWeight;
    double firstWeight = _centroidWeights[0];
    if (firstWeight < 1.0 && index < firstWeight / 2.0) {
      return weightedAverage(_min, firstWeight / 2.0 - index, _centroidMeans[0], index);
    }
    if (firstWeight >= 1.0 && index < 1.0) {
      return _min;
    }
    if (_centroidWeights[0] > 1.0 && index < _centroidWeights[0] / 2.0) {
      return _min + (index - 1.0) / (_centroidWeights[0] / 2.0 - 1.0) * (_centroidMeans[0] - _min);
    }
    int lastIndex = _numCentroids - 1;
    double lastWeight = _centroidWeights[lastIndex];
    if (lastWeight < 1.0 && index >= _totalWeight - lastWeight / 2.0) {
      return weightedAverage(_centroidMeans[lastIndex], _totalWeight - index, _max,
          index - (_totalWeight - lastWeight / 2.0));
    }
    if (lastWeight >= 1.0 && index > _totalWeight - 1.0) {
      return _max;
    }
    if (_centroidWeights[lastIndex] > 1.0
        && _totalWeight - index <= _centroidWeights[lastIndex] / 2.0) {
      return _max - (_totalWeight - index - 1.0) / (_centroidWeights[lastIndex] / 2.0 - 1.0)
          * (_max - _centroidMeans[lastIndex]);
    }

    double weightSoFar = _centroidWeights[0] / 2.0;
    for (int i = 0; i < lastIndex; i++) {
      double halfWeight = (_centroidWeights[i] + _centroidWeights[i + 1]) / 2.0;
      if (weightSoFar + halfWeight > index) {
        double leftExcludedWeight = getPointMassHalfWeight(i);
        if (index - weightSoFar < leftExcludedWeight) {
          return _centroidMeans[i];
        }
        double rightExcludedWeight = getPointMassHalfWeight(i + 1);
        if (weightSoFar + halfWeight - index <= rightExcludedWeight) {
          return _centroidMeans[i + 1];
        }
        double leftWeight = index - weightSoFar - leftExcludedWeight;
        double rightWeight = weightSoFar + halfWeight - index - rightExcludedWeight;
        return weightedAverage(_centroidMeans[i], rightWeight, _centroidMeans[i + 1], leftWeight);
      }
      weightSoFar += halfWeight;
    }
    double weightToMax = index - _totalWeight - _centroidWeights[lastIndex] / 2.0;
    double weightFromLastCentroid = _centroidWeights[lastIndex] / 2.0 - weightToMax;
    return weightedAverage(_centroidMeans[lastIndex], weightToMax, _max, weightFromLastCentroid);
  }

  private double getPointMassHalfWeight(int index) {
    // Adjacent equal means establish a repeated-value plateau. Keep its complete mass out of interpolation at
    // either edge; an isolated weighted centroid still represents an unknown distribution around its mean.
    double mean = _centroidMeans[index];
    boolean pointMass = _centroidWeights[index] == 1.0 || index > 0 && _centroidMeans[index - 1] == mean
        || index + 1 < _numCentroids && _centroidMeans[index + 1] == mean;
    return pointMass ? _centroidWeights[index] / 2.0 : 0.0;
  }

  private static double weightedAverage(double firstValue, double firstWeight, double secondValue,
      double secondWeight) {
    if (firstValue > secondValue) {
      return weightedAverage(secondValue, secondWeight, firstValue, firstWeight);
    }
    if (firstWeight == 0.0) {
      return secondValue;
    }
    if (secondWeight == 0.0 || firstValue == secondValue) {
      return firstValue;
    }
    if (!Double.isFinite(firstValue)) {
      return firstValue;
    }
    if (!Double.isFinite(secondValue)) {
      return secondValue;
    }
    double average = (firstValue * firstWeight + secondValue * secondWeight) / (firstWeight + secondWeight);
    return Math.max(firstValue, Math.min(average, secondValue));
  }

  @Override
  public Collection<Centroid> centroids() {
    if (_legacyDegraded) {
      byte[] bytes = serialize();
      ByteBuffer encoded = ByteBuffer.wrap(bytes);
      SerializedTDigestMetadata metadata = TDigestUtils.readSerializedHeader(encoded);
      encoded.position(metadata.centroidOffset());
      List<Centroid> centroids = new ArrayList<>(metadata.centroidCount());
      for (int i = 0; i < metadata.centroidCount(); i++) {
        double weight = metadata.centroidSize() == TDigestUtils.VERBOSE_CENTROID_SIZE
            ? encoded.getDouble() : encoded.getFloat();
        double mean = metadata.centroidSize() == TDigestUtils.VERBOSE_CENTROID_SIZE
            ? encoded.getDouble() : encoded.getFloat();
        centroids.add(new Centroid(mean, weight));
      }
      return centroids;
    }
    materializePendingSerializedTDigest();
    flush();
    List<Centroid> centroids = new ArrayList<>(_numCentroids);
    for (int i = 0; i < _numCentroids; i++) {
      centroids.add(new Centroid(_centroidMeans[i], _centroidWeights[i]));
    }
    return centroids;
  }

  /// Copies a valid distribution's primitive centroid arrays after flushing buffered inputs. This does not apply
  /// public compression or serialize a finite sub-distribution; callers can include surrounding infinity mass.
  public void copyCentroids(double[] means, double[] weights, int offset) {
    if (_legacyDegraded) {
      throw corruptedMutation();
    }
    materializePendingSerializedTDigest();
    flush();
    if (means == weights || offset < 0 || offset > means.length - _numCentroids
        || offset > weights.length - _numCentroids) {
      throw new IllegalArgumentException("Insufficient distinct TDigest centroid arrays");
    }
    if (_numCentroids == 0) {
      return;
    }
    System.arraycopy(_centroidMeans, 0, means, offset, _numCentroids);
    System.arraycopy(_centroidWeights, 0, weights, offset, _numCentroids);
  }

  @Override
  public double compression() {
    return _compression;
  }

  /// Returns the length of the [#serialize()] bytes rather than the raw verbose byte size, so generic `TDigest`
  /// serializers ([#byteSize()] followed by [#asBytes(ByteBuffer)]) emit the mixed-version-compatible encoding.
  /// Prepared bytes are cached until the next write or mutation, avoiding a second encoding pass for that pattern.
  /// Emitting raw verbose bytes here could produce a digest with more centroids than an old reader of the same
  /// compression allocates, which it rejects with [ArrayIndexOutOfBoundsException].
  @Override
  public int byteSize() {
    if (_legacyDegraded || _originalFractionalBytes != null) {
      return _legacyDegraded ? _pendingSerializedTDigest.length : _originalFractionalBytes.length;
    }
    if (_serializedBytesForWrite == null) {
      _serializedBytesForWrite = serialize();
    }
    return _serializedBytesForWrite.length;
  }

  /// Bounds both compatible encodings and a widened verbose view without flushing. Buffered and fractional inputs
  /// can reserve more space than their eventual bytes; retained capacities need not fit default compression size.
  @Override
  public int maxSerializedByteSize() {
    if (_legacyDegraded) {
      return _pendingSerializedTDigest.length;
    }
    if (_originalFractionalBytes != null) {
      return _originalFractionalBytes.length;
    }
    long centroidCount = getCentroidCountUpperBound();
    if (!_hasFractionalWeights) {
      centroidCount = (long) Math.min(centroidCount, Math.ceil(getTotalWeight()));
    }
    int maximumBytes = getMaxVerboseByteSize(centroidCount);
    return _pendingSerializedTDigest == null ? maximumBytes : Math.max(maximumBytes, _pendingSerializedTDigest.length);
  }

  private static int getMaxVerboseByteSize(long centroidCount) {
    return Math.toIntExact(Math.addExact(TDigestUtils.VERBOSE_HEADER_SIZE,
        Math.multiplyExact(TDigestUtils.VERBOSE_CENTROID_SIZE, centroidCount)));
  }

  /// Returns the small-encoding size unless float narrowing would overflow, in which case verbose bytes are used.
  /// Historical degraded state retains its original encoding and rejects mutation.
  @Override
  public int smallByteSize() {
    if (_legacyDegraded || _originalFractionalBytes != null) {
      return _legacyDegraded ? _pendingSerializedTDigest.length : _originalFractionalBytes.length;
    }
    compress();
    normalizeBoundaryCentroids();
    checkFractionalBoundaryEncoding();
    checkCapacityPreservingCentroidCount();
    if (!canUseSmallEncoding()) {
      return byteSize();
    }
    return Math.addExact(TDigestUtils.SMALL_HEADER_SIZE,
        Math.multiplyExact(TDigestUtils.SMALL_CENTROID_SIZE, _numCentroids));
  }

  /// Writes the [#serialize()] bytes; see [#byteSize()]. Bytes are always written in big-endian order (the t-digest
  /// wire order) regardless of the destination buffer's byte order, unlike the library's `asBytes`.
  @Override
  public void asBytes(ByteBuffer buffer) {
    byte[] bytes = _serializedBytesForWrite;
    _serializedBytesForWrite = null;
    buffer.put(bytes != null ? bytes : serialize());
  }

  /// Writes the representation described by [#smallByteSize()]; readers must inspect the leading encoding identifier.
  @Override
  public void asSmallBytes(ByteBuffer buffer) {
    if (_legacyDegraded || _originalFractionalBytes != null) {
      buffer.put(serialize());
      return;
    }
    compress();
    normalizeBoundaryCentroids();
    checkFractionalBoundaryEncoding();
    checkCapacityPreservingCentroidCount();
    if (canUseSmallEncoding()) {
      buffer.put(toCapacityPreservingBytes());
    } else {
      asBytes(buffer);
    }
  }

  @Override
  public int centroidCount() {
    if (_pendingSerializedTDigest != null) {
      return !_legacyDegraded && _pendingSerializedMetadata.totalWeight() == 0.0
          ? 0 : _pendingSerializedMetadata.centroidCount();
    }
    flush();
    return _numCentroids;
  }

  @Override
  public double getMin() {
    if (_pendingSerializedTDigest != null) {
      return !_legacyDegraded && _pendingSerializedMetadata.totalWeight() == 0.0
          ? Double.POSITIVE_INFINITY : _pendingSerializedMetadata.min();
    }
    flush();
    return _min;
  }

  @Override
  public double getMax() {
    if (_pendingSerializedTDigest != null) {
      return !_legacyDegraded && _pendingSerializedMetadata.totalWeight() == 0.0
          ? Double.NEGATIVE_INFINITY : _pendingSerializedMetadata.max();
    }
    flush();
    return _max;
  }

  public byte[] serialize() {
    if (_legacyDegraded) {
      return _pendingSerializedTDigest.clone();
    }
    if (_originalFractionalBytes != null) {
      return _originalFractionalBytes.clone();
    }
    if (_pendingSerializedTDigest != null) {
      if (_pendingSerializedMetadata.weightedBoundaries() || _pendingSerializedMetadata.hasZeroWeightCentroids()) {
        materializePendingSerializedTDigest();
      } else {
        if (ByteBuffer.wrap(_pendingSerializedTDigest).getInt() == TDigestUtils.VERBOSE_ENCODING) {
          byte[] serialized = TDigestUtils.makeLegacyCompatible(_pendingSerializedTDigest, _pendingSerializedMetadata);
          return serialized == _pendingSerializedTDigest ? serialized.clone() : serialized;
        }
        return _pendingSerializedTDigest.clone();
      }
    }
    compress();
    normalizeBoundaryCentroids();
    checkFractionalBoundaryEncoding();
    return toLegacyCompatibleVerboseBytes();
  }

  private void recompressCentroids(double compression) {
    normalizeBoundaryCentroids();
    double[] means = _centroidMeans;
    double[] weights = _centroidWeights;
    int numCentroids = _numCentroids;
    double totalWeight = _totalWeight;
    _numCentroids = 0;
    _totalWeight = 0.0;
    boolean runBackwards = (_mergeCount++ & 1) != 0;
    if (!mergeSorted(means, weights, numCentroids, totalWeight, compression, runBackwards)) {
      throw new IllegalStateException("Cannot recompress an empty TDigest");
    }
  }

  private byte[] toLegacyCompatibleVerboseBytes() {
    ByteBuffer buffer = ByteBuffer.allocate(
        Math.addExact(TDigestUtils.VERBOSE_HEADER_SIZE,
            Math.multiplyExact(TDigestUtils.VERBOSE_CENTROID_SIZE, _numCentroids)));
    buffer.putInt(TDigestUtils.VERBOSE_ENCODING);
    buffer.putDouble(_min);
    buffer.putDouble(_max);
    buffer.putDouble(_compression);
    buffer.putInt(_numCentroids);
    double encodedWeight = 0.0;
    boolean hasNonFiniteMeans = false;
    boolean fractionalWeights = false;
    for (int i = 0; i < _numCentroids; i++) {
      buffer.putDouble(_centroidWeights[i]);
      buffer.putDouble(_centroidMeans[i]);
      encodedWeight += _centroidWeights[i];
      hasNonFiniteMeans |= !Double.isFinite(_centroidMeans[i]);
      fractionalWeights |= _centroidWeights[i] != Math.rint(_centroidWeights[i]);
    }
    checkTotalWeight(encodedWeight);
    boolean weightedBoundaries = _numCentroids == 1 ? _centroidWeights[0] >= 2.0
        : _numCentroids > 1 && (_centroidWeights[0] > 1.0 || _centroidWeights[_numCentroids - 1] > 1.0);
    // Collect exact wire metadata during the write rather than decoding and validating our fresh bytes again.
    SerializedTDigestMetadata metadata = new SerializedTDigestMetadata(TDigestUtils.VERBOSE_ENCODING, _min, _max,
        _compression, _numCentroids, Math.max(TDigestUtils.getDefaultCentroidCapacity(_compression),
        TDigestUtils.getLegacyDefaultCentroidCapacity(_compression)), 0,
        TDigestUtils.VERBOSE_HEADER_SIZE, TDigestUtils.VERBOSE_CENTROID_SIZE, buffer.capacity(), encodedWeight,
        hasNonFiniteMeans, false, false, fractionalWeights, weightedBoundaries, false, _compression);
    return TDigestUtils.makeLegacyCompatible(buffer.array(), metadata);
  }

  private boolean canUseSmallEncoding() {
    if (!Float.isFinite((float) _compression)) {
      return false;
    }
    for (int i = 0; i < _numCentroids; i++) {
      if (!Float.isFinite((float) _centroidWeights[i])
          || Double.isFinite(_centroidMeans[i]) && !Float.isFinite((float) _centroidMeans[i])) {
        return false;
      }
    }
    return true;
  }

  private byte[] toCapacityPreservingBytes() {
    checkCapacityPreservingCentroidCount();
    int mainCapacity = Math.min(Short.MAX_VALUE,
        Math.max(_centroidCapacity, Math.max(_numCentroids, _serializedMainCapacity)));
    long defaultBufferCapacity = Math.multiplyExact(TDigestUtils.DEFAULT_MERGE_BUFFER_MULTIPLIER, (long) mainCapacity);
    int bufferCapacity = Math.toIntExact(Math.min(Short.MAX_VALUE,
        Math.max(Math.max((long) _serializedBufferCapacity, mainCapacity + 1L), defaultBufferCapacity)));
    ByteBuffer buffer = ByteBuffer.allocate(
        TDigestUtils.SMALL_HEADER_SIZE + TDigestUtils.SMALL_CENTROID_SIZE * _numCentroids);
    buffer.putInt(TDigestUtils.SMALL_ENCODING);
    buffer.putDouble(_min);
    buffer.putDouble(_max);
    buffer.putFloat((float) _compression);
    buffer.putShort((short) mainCapacity);
    buffer.putShort((short) bufferCapacity);
    buffer.putShort((short) _numCentroids);
    for (int i = 0; i < _numCentroids; i++) {
      buffer.putFloat((float) _centroidWeights[i]);
      buffer.putFloat((float) _centroidMeans[i]);
    }
    return buffer.array();
  }

  private void checkCapacityPreservingCentroidCount() {
    if (_numCentroids > Short.MAX_VALUE) {
      throw new IllegalStateException("TDigest has too many centroids for capacity-preserving encoding: "
          + _numCentroids);
    }
  }

  private void checkFractionalBoundaryEncoding() {
    if (_numCentroids > 0) {
      TDigestUtils.checkLegacyBoundaryWeights(_numCentroids, _centroidWeights[0], _centroidWeights[_numCentroids - 1]);
    }
  }

  private void flush() {
    // SQL accumulators merge raw inputs incrementally at configured compression. General digest aggregation keeps
    // the legacy two-level raw merge, preserving sparse Star-tree query accuracy. Buffered reducer inputs also
    // retain the two-level merge for its accuracy benefit.
    flush(_numIncomingCentroids == 0 && !_useTwoLevelRawCompression
        ? getIncrementalCompression()
        : _workingCompression);
  }

  private double getIncrementalCompression() {
    // Very low compression leaves too little centroid headroom for incremental public-compression merges to retain
    // t-digest 3.3's accuracy. Keep the two-level strategy for those uncommon configurations.
    return _compression >= MIN_INCREMENTAL_COMPRESSION ? _compression : _workingCompression;
  }

  private void flush(double compression) {
    if (_numIncomingCentroids > 0) {
      flushIncoming(compression);
      return;
    }
    if (_numRawValues == 0) {
      return;
    }

    Arrays.sort(_rawValues, 0, _numRawValues);
    double rawMin = _rawValues[0];
    double rawMax = _rawValues[_numRawValues - 1];
    boolean runBackwards = (_mergeCount++ & 1) != 0;
    mergeSorted(_rawValues, null, _numRawValues, _numRawValues, compression, runBackwards);
    _numRawValues = 0;
    _min = Math.min(_min, rawMin);
    _max = Math.max(_max, rawMax);
  }

  private void flushIncoming(double compression) {
    // Match mergeSorted's association before rearranging raw or paired centroid buffers.
    checkTotalWeight(_totalWeight + (_incomingWeight + _numRawValues));
    double rawMin = Double.POSITIVE_INFINITY;
    double rawMax = Double.NEGATIVE_INFINITY;
    if (_numRawValues > 0) {
      Arrays.sort(_rawValues, 0, _numRawValues);
      rawMin = _rawValues[0];
      rawMax = _rawValues[_numRawValues - 1];
      ensureIncomingCapacity(Math.addExact(_numIncomingCentroids, _numRawValues));
      System.arraycopy(_incomingMeans, 0, _incomingMeans, _numRawValues, _numIncomingCentroids);
      System.arraycopy(_incomingWeights, 0, _incomingWeights, _numRawValues, _numIncomingCentroids);
      for (int i = 0; i < _numRawValues; i++) {
        _incomingMeans[i] = _rawValues[i];
        _incomingWeights[i] = 1.0;
      }
      _numIncomingCentroids += _numRawValues;
      _incomingWeight += _numRawValues;
      _numRawValues = 0;
      _incomingCentroidsSorted = false;
    }

    int incomingCount = _numIncomingCentroids;
    double incomingWeight = _incomingWeight;
    double[] incomingMeans = _incomingMeans;
    double[] incomingWeights = _incomingWeights;
    if (!_incomingCentroidsSorted) {
      ensureSortedIncomingCapacity(incomingCount);
      stableSortIncoming(incomingCount);
      incomingMeans = _incomingMeans;
      incomingWeights = _incomingWeights;
      _incomingCentroidsSorted = true;
    }
    boolean runBackwards = (_mergeCount & 1) != 0;
    if (!mergeSorted(incomingMeans, incomingWeights, incomingCount, incomingWeight, compression, runBackwards)) {
      throw new IllegalStateException("Cannot flush an empty TDigest input buffer");
    }
    _mergeCount++;
    _numIncomingCentroids = 0;
    _incomingWeight = 0.0;
    _incomingCentroidsSorted = true;
    _min = Math.min(_min, rawMin);
    _max = Math.max(_max, rawMax);
  }

  private boolean mergeSorted(double[] incomingMeans, double[] incomingWeights, int incomingCount,
      double incomingWeight, double compression, boolean runBackwards) {
    if (incomingWeight == 0.0) {
      return false;
    }

    double newTotalWeight = _totalWeight + incomingWeight;
    checkTotalWeight(newTotalWeight);
    normalizeBoundaryCentroids();
    int preferredOutputCapacity = getPreferredOutputCapacity(incomingCount);
    ensureOutputCapacity(1);
    double totalWeightNormalizer = 1.0 / newTotalWeight;
    double weightNormalizer = totalWeightNormalizer / getK1MaxWeightScale(compression);
    int incomingIndex = runBackwards ? incomingCount - 1 : 0;
    int centroidIndex = runBackwards ? _numCentroids - 1 : 0;
    int incomingLimit = runBackwards ? -1 : incomingCount;
    int centroidLimit = runBackwards ? -1 : _numCentroids;
    int direction = runBackwards ? -1 : 1;
    int numInputs = incomingCount + _numCentroids;
    int inputIndex = 0;
    int numOutputCentroids = 0;
    double weightSoFar = 0.0;
    double firstInputMean = _numCentroids == 0 ? incomingMeans[0]
        : Math.min(incomingMeans[0], _centroidMeans[0]);
    double lastInputMean = _numCentroids == 0 ? incomingMeans[incomingCount - 1]
        : Math.max(incomingMeans[incomingCount - 1], _centroidMeans[_numCentroids - 1]);
    boolean allFiniteMeans = Double.isFinite(firstInputMean) && Double.isFinite(lastInputMean);
    boolean sameSignMeans = firstInputMean > 0.0 || lastInputMean < 0.0;
    while (incomingIndex != incomingLimit || centroidIndex != centroidLimit) {
      double mean;
      double weight;
      boolean takeIncoming = centroidIndex == centroidLimit || (incomingIndex != incomingLimit
          && (runBackwards ? incomingMeans[incomingIndex] > _centroidMeans[centroidIndex]
              : incomingMeans[incomingIndex] <= _centroidMeans[centroidIndex]));
      if (takeIncoming) {
        mean = incomingMeans[incomingIndex];
        weight = incomingWeights == null ? 1.0 : incomingWeights[incomingIndex];
        incomingIndex += direction;
      } else {
        mean = _centroidMeans[centroidIndex];
        weight = _centroidWeights[centroidIndex];
        centroidIndex += direction;
      }
      if (numOutputCentroids == 0) {
        _outputMeans[0] = mean;
        _outputWeights[0] = weight;
        numOutputCentroids = 1;
      } else {
        int currentIndex = numOutputCentroids - 1;
        double proposedWeight = _outputWeights[currentIndex] + weight;
        double q0 = weightSoFar * totalWeightNormalizer;
        double q2 = (weightSoFar + proposedWeight) * totalWeightNormalizer;
        double normalizedWeight = proposedWeight * weightNormalizer;
        double normalizedWeightSquared = normalizedWeight * normalizedWeight;
        boolean canMerge = (allFiniteMeans || canMergeMeans(_outputMeans[currentIndex], mean))
            && normalizedWeightSquared <= q0 * (1.0 - q0)
            && normalizedWeightSquared <= q2 * (1.0 - q2);
        if (inputIndex == 1 || inputIndex == numInputs - 1) {
          canMerge = false;
        }
        if (canMerge) {
          _outputWeights[currentIndex] = proposedWeight;
          _outputMeans[currentIndex] =
              TDigestUtils.weightedMean(_outputMeans[currentIndex], proposedWeight - weight, mean, weight,
                  proposedWeight, sameSignMeans);
        } else {
          weightSoFar += _outputWeights[currentIndex];
          ensureOutputCapacity(Math.max(preferredOutputCapacity, numOutputCentroids + 1));
          _outputMeans[numOutputCentroids] = mean;
          _outputWeights[numOutputCentroids] = weight;
          numOutputCentroids++;
        }
      }
      inputIndex++;
    }
    if (runBackwards) {
      reverse(_outputMeans, numOutputCentroids);
      reverse(_outputWeights, numOutputCentroids);
    }
    finishMerge(numOutputCentroids, newTotalWeight, compression);
    return true;
  }

  private void finishMerge(int numOutputCentroids, double totalWeight, double compression) {
    double[] temporary = _centroidMeans;
    _centroidMeans = _outputMeans;
    _outputMeans = temporary;
    temporary = _centroidWeights;
    _centroidWeights = _outputWeights;
    _outputWeights = temporary;
    _numCentroids = numOutputCentroids;
    _totalWeight = totalWeight;
    _publiclyCompressed = compression == _compression;
  }

  private void ensureIncomingCapacity(int capacity) {
    if (_incomingMeans == null || capacity > _incomingMeans.length) {
      int newCapacity = _incomingMeans == null
          ? Math.max(capacity, Math.min(_centroidCapacity, _pendingCentroidCapacity))
          : Math.max(capacity, (int) Math.min(_pendingCentroidCapacity,
              (long) _incomingMeans.length * TDigestUtils.DEFAULT_MERGE_BUFFER_MULTIPLIER));
      _incomingMeans = _incomingMeans == null ? new double[newCapacity] : Arrays.copyOf(_incomingMeans, newCapacity);
      _incomingWeights =
          _incomingWeights == null ? new double[newCapacity] : Arrays.copyOf(_incomingWeights, newCapacity);
    }
  }

  private void stableSortIncoming(int count) {
    double[] sourceMeans = _incomingMeans;
    double[] sourceWeights = _incomingWeights;
    double[] destinationMeans = _sortedIncomingMeans;
    double[] destinationWeights = _sortedIncomingWeights;
    for (int width = 1; width < count; width *= 2) {
      for (int start = 0; start < count; start += 2 * width) {
        int middle = Math.min(start + width, count);
        int end = Math.min(start + 2 * width, count);
        int left = start;
        int right = middle;
        int output = start;
        while (left < middle && right < end) {
          // Equal means keep their input order, including equal signed zeros.
          int source = sourceMeans[left] <= sourceMeans[right] ? left++ : right++;
          destinationMeans[output] = sourceMeans[source];
          destinationWeights[output++] = sourceWeights[source];
        }
        int remaining = left < middle ? middle - left : end - right;
        int source = left < middle ? left : right;
        System.arraycopy(sourceMeans, source, destinationMeans, output, remaining);
        System.arraycopy(sourceWeights, source, destinationWeights, output, remaining);
      }
      double[] temporary = sourceMeans;
      sourceMeans = destinationMeans;
      destinationMeans = temporary;
      temporary = sourceWeights;
      sourceWeights = destinationWeights;
      destinationWeights = temporary;
    }
    _incomingMeans = sourceMeans;
    _incomingWeights = sourceWeights;
    _sortedIncomingMeans = destinationMeans;
    _sortedIncomingWeights = destinationWeights;
  }

  private void ensureSortedIncomingCapacity(int capacity) {
    if (_sortedIncomingMeans == null || capacity > _sortedIncomingMeans.length) {
      int newCapacity = _sortedIncomingMeans == null
          ? Math.max(capacity, Math.min(_centroidCapacity, _pendingCentroidCapacity))
          : Math.max(capacity, Math.multiplyExact(_sortedIncomingMeans.length, 2));
      _sortedIncomingMeans = new double[newCapacity];
      _sortedIncomingWeights = new double[newCapacity];
    }
  }

  private void bufferIncomingCentroid(double mean, double weight) {
    checkTotalWeight(_totalWeight + _incomingWeight + _numRawValues + weight);
    if (_numRawValues + _numIncomingCentroids == getPendingInputLimit()) {
      flush();
    }
    ensureIncomingCapacity(_numIncomingCentroids + 1);
    if (_incomingCentroidsSorted && _numIncomingCentroids > 0
        && mean < _incomingMeans[_numIncomingCentroids - 1]) {
      _incomingCentroidsSorted = false;
    }
    _incomingMeans[_numIncomingCentroids] = mean;
    _incomingWeights[_numIncomingCentroids] = weight;
    _numIncomingCentroids++;
    _incomingWeight += weight;
  }

  private void bufferIncomingCentroids(double[] means, double[] weights, int count) {
    int offset = 0;
    while (offset < count) {
      if (_numRawValues + _numIncomingCentroids == getPendingInputLimit()) {
        flush();
      }
      int copied = Math.min(count - offset,
          getPendingInputLimit() - _numRawValues - _numIncomingCentroids);
      int existingCount = _numIncomingCentroids;
      int combinedCount = Math.addExact(existingCount, copied);
      if (existingCount == 0 || !_incomingCentroidsSorted) {
        ensureIncomingCapacity(combinedCount);
        System.arraycopy(means, offset, _incomingMeans, existingCount, copied);
        System.arraycopy(weights, offset, _incomingWeights, existingCount, copied);
      } else {
        ensureIncomingCapacity(combinedCount);
        int existingIndex = existingCount - 1;
        int addedIndex = offset + copied - 1;
        int outputIndex = combinedCount - 1;
        while (existingIndex >= 0 && addedIndex >= offset) {
          // Choose the added centroid on equality while merging backwards so existing equal centroids retain stable
          // order before the newly added run.
          if (_incomingMeans[existingIndex] > means[addedIndex]) {
            _incomingMeans[outputIndex] = _incomingMeans[existingIndex];
            _incomingWeights[outputIndex--] = _incomingWeights[existingIndex--];
          } else {
            _incomingMeans[outputIndex] = means[addedIndex];
            _incomingWeights[outputIndex--] = weights[addedIndex--];
          }
        }
        if (addedIndex >= offset) {
          int remaining = addedIndex - offset + 1;
          System.arraycopy(means, offset, _incomingMeans, 0, remaining);
          System.arraycopy(weights, offset, _incomingWeights, 0, remaining);
        }
      }
      for (int i = 0; i < copied; i++) {
        _incomingWeight += weights[offset + i];
      }
      offset += copied;
      _numIncomingCentroids = combinedCount;
    }
  }

  private void materializePendingSerializedTDigest() {
    if (_pendingSerializedTDigest != null && !_legacyDegraded) {
      _serializedBytesForWrite = null;
      byte[] bytes = _pendingSerializedTDigest;
      SerializedTDigestMetadata metadata = _pendingSerializedMetadata;
      _pendingSerializedTDigest = null;
      _pendingSerializedMetadata = null;
      SerializedTDigestInput input = getSerializedTDigestInput();
      input.reset(bytes, metadata);
      input._retainedBytes = bytes;
      input.decode();
      mergeSerializedTDigest(input, false);
    }
  }

  private void markLegacyDegraded(SerializedTDigestInput input) {
    if (!_legacyDegraded) {
      LOGGER.warn("Retaining historical TDigest numerical corruption: input centroids={}, input weight={}, "
              + "accumulated weight={}. Percentiles for this aggregation state are NaN.",
          input._numCentroids, input._totalWeight, _totalWeight + _incomingWeight + _numRawValues);
      _legacyDegraded = true;
    }
  }

  private void ensureCentroidCapacity(int capacity) {
    if (capacity > 0 && (_centroidMeans == null || capacity > _centroidMeans.length)) {
      _centroidMeans = new double[capacity];
      _centroidWeights = new double[capacity];
    }
  }

  private void normalizeBoundaryCentroids() {
    if (_numCentroids == 0 || (_centroidWeights[0] == 1.0
        && _centroidWeights[_numCentroids - 1] == 1.0)) {
      return;
    }
    int requiredCapacity = Math.addExact(_numCentroids, 2);
    if (_centroidMeans.length < requiredCapacity) {
      _centroidMeans = Arrays.copyOf(_centroidMeans, requiredCapacity);
      _centroidWeights = Arrays.copyOf(_centroidWeights, requiredCapacity);
    }
    _numCentroids = normalizeBoundaries(_centroidMeans, _centroidWeights, _numCentroids, _min, _max);
  }

  private static int normalizeBoundaries(double[] means, double[] weights, int count, double min, double max) {
    if (count == 0 || (weights[0] == 1.0 && weights[count - 1] == 1.0)) {
      return count;
    }
    if (count == 1) {
      double weight = weights[0];
      if (weight < 2.0) {
        return count;
      }
      double mean = means[0];
      means[0] = min;
      weights[0] = 1.0;
      if (weight == 2.0) {
        means[1] = max;
        weights[1] = 1.0;
        return 2;
      }
      means[1] = residualMean(mean, weight, min, max, weight - 2.0, min, max);
      weights[1] = weight - 2.0;
      means[2] = max;
      weights[2] = 1.0;
      return 3;
    }

    if (weights[0] > 1.0) {
      System.arraycopy(means, 1, means, 2, count - 1);
      System.arraycopy(weights, 1, weights, 2, count - 1);
      double weight = weights[0];
      double mean = means[0];
      means[0] = min;
      weights[0] = 1.0;
      means[1] = residualMean(mean, weight, min, 0.0, weight - 1.0, min, means[2]);
      weights[1] = weight - 1.0;
      count++;
    }
    int lastIndex = count - 1;
    if (weights[lastIndex] > 1.0) {
      double weight = weights[lastIndex];
      double mean = means[lastIndex];
      means[lastIndex] = residualMean(mean, weight, max, 0.0, weight - 1.0, means[lastIndex - 1], max);
      weights[lastIndex] = weight - 1.0;
      means[count] = max;
      weights[count] = 1.0;
      count++;
    }
    return count;
  }

  private void ensureRawBuffer() {
    if (_rawValues == null) {
      _rawValues = new double[getRawBufferSize((long) Math.ceil(_compression))];
    }
  }

  private static int getRawBufferSize(long roundedCompression) {
    return (int) Math.min(MAX_RAW_BUFFER_SIZE, Math.max(MIN_RAW_BUFFER_SIZE, 2.0 * roundedCompression));
  }

  private static int getPendingCentroidCapacity(int centroidCapacity) {
    return (int) Math.min(MAX_RAW_BUFFER_SIZE,
        Math.max(MIN_RAW_BUFFER_SIZE, (long) TDigestUtils.DEFAULT_MERGE_BUFFER_MULTIPLIER * centroidCapacity));
  }

  private int getPendingInputLimit() {
    // Large compression can retain more centroids than this independent buffer's cap. Keep batching instead of
    // shrinking to one new input per full merge as the accumulated centroid count grows.
    if (_pendingCentroidCapacity == MAX_RAW_BUFFER_SIZE) {
      return MAX_RAW_BUFFER_SIZE;
    }
    return Math.max(1, _pendingCentroidCapacity - _numCentroids - 1);
  }

  private void preserveSerializedCapacity(int mainCapacity, int bufferCapacity) {
    if (mainCapacity > 0) {
      _serializedMainCapacity = Math.max(_serializedMainCapacity, mainCapacity);
    }
    if (bufferCapacity > 0) {
      _serializedBufferCapacity = Math.max(_serializedBufferCapacity, bufferCapacity);
    }
  }

  private int getPreferredOutputCapacity(int incomingCount) {
    return Math.min(_centroidCapacity, Math.addExact(_numCentroids, incomingCount));
  }

  private void ensureOutputCapacity(int capacity) {
    if (_outputMeans == null) {
      int newCapacity = Math.max(capacity, Math.min(_centroidCapacity, MIN_RAW_BUFFER_SIZE));
      _outputMeans = new double[newCapacity];
      _outputWeights = new double[newCapacity];
    } else if (capacity > _outputMeans.length) {
      int newCapacity = Math.max(capacity, Math.multiplyExact(_outputMeans.length, 2));
      _outputMeans = Arrays.copyOf(_outputMeans, newCapacity);
      _outputWeights = Arrays.copyOf(_outputWeights, newCapacity);
    }
  }

  private static void checkTotalWeight(double totalWeight) {
    if (!(totalWeight >= 0.0) || !Double.isFinite(totalWeight)) {
      throw new IllegalArgumentException("Invalid TDigest total weight: " + totalWeight);
    }
  }

  private void requireMutable() {
    if (_legacyDegraded) {
      throw corruptedMutation();
    }
    _serializedBytesForWrite = null;
  }

  private static IllegalArgumentException corruptedMutation() {
    return new IllegalArgumentException("Cannot merge or mutate a historically corrupted TDigest; "
        + "only its unchanged original bytes can be retained");
  }

  private static double positiveCompression(double compression) {
    if (!(compression > 0.0)) {
      throw new IllegalArgumentException("TDigest compression must be positive: " + compression);
    }
    return compression;
  }

  private double getK1MaxWeightScale(double compression) {
    return compression == _compression ? _publicMaxWeightScale : _workingMaxWeightScale;
  }

  private static double calculateK1MaxWeightScale(double compression) {
    return 2.0 * Math.sin(Math.PI / compression);
  }

  private static void reverse(double[] values, int length) {
    for (int i = 0; i < length / 2; i++) {
      int otherIndex = length - i - 1;
      double value = values[i];
      values[i] = values[otherIndex];
      values[otherIndex] = value;
    }
  }

  private static double clamp(double value, double min, double max) {
    return Math.max(min, Math.min(value, max));
  }

  private static boolean canMergeMeans(double firstMean, double secondMean) {
    return firstMean == secondMean || (Double.isFinite(firstMean) && Double.isFinite(secondMean));
  }

  private static double residualMean(double mean, double weight, double firstRemovedValue,
      double secondRemovedValue, double residualWeight, double min, double max) {
    if (!Double.isFinite(mean) || !Double.isFinite(firstRemovedValue)
        || !Double.isFinite(secondRemovedValue)) {
      return clamp(mean, min, max);
    }
    boolean removedTwoValues = weight - residualWeight == 2.0;
    int scaleShift = removedTwoValues ? 2 : 1;
    double scaledMean = Math.scalb(mean, -scaleShift);
    double scaledCorrection = scaledMean - Math.scalb(firstRemovedValue, -scaleShift);
    if (removedTwoValues) {
      scaledCorrection += scaledMean - Math.scalb(secondRemovedValue, -scaleShift);
    }
    double correction = Math.scalb(scaledCorrection / residualWeight, scaleShift);
    return clamp(mean + correction, min, max);
  }

  private static double readCompression(byte[] bytes) {
    return TDigestUtils.readSerializedHeader(ByteBuffer.wrap(bytes)).compression();
  }

  /// Reusable, invocation-local view of one serialized TDigest for group-by-MV fanout.
  ///
  /// [#reset(byte\[\])] validates the header once per input row. The first decode combines numerical validation
  /// with filling reusable centroid arrays, so all groups for a row merge the same primitive input without sharing
  /// mutable accumulator state. A pending accumulator requests an immutable byte snapshot, created at most once per
  /// reset
  /// and shared by the row's group fanout. The input must remain thread-confined and must not outlive the aggregation
  /// call that owns it. Pending-only inputs inspect their bytes without allocating centroid arrays; their first
  /// later materialization reads the already validated values without another numerical validation pass.
  public static final class SerializedTDigestInput {
    private byte[] _bytes;
    private byte[] _retainedBytes;
    private double _min;
    private double _max;
    private double _compression;
    private int _numCentroids;
    private int _mainCapacity;
    private int _bufferCapacity;
    private int _centroidOffset;
    private int _centroidSize;
    private double[] _means;
    private double[] _weights;
    private double _totalWeight;
    private boolean _decoded;

    private SerializedTDigestMetadata _metadata;

    public void reset(byte[] bytes) {
      reset(bytes, TDigestUtils.readSerializedHeader(ByteBuffer.wrap(bytes)));
    }

    /// Owns one snapshot of the encoded digest and advances the source buffer past it.
    public void reset(ByteBuffer bytes) {
      reset(bytes, true);
    }

    /// Reads a source digest, optionally enforcing the historical decoder's declared centroid capacity.
    public void reset(ByteBuffer bytes, boolean checkCapacity) {
      SerializedTDigestMetadata metadata = TDigestUtils.readSerializedHeader(bytes, checkCapacity);
      byte[] snapshot = new byte[metadata.encodedLength()];
      bytes.get(snapshot);
      reset(snapshot, metadata);
      _retainedBytes = snapshot;
    }

    private void reset(byte[] bytes, SerializedTDigestMetadata metadata) {
      _metadata = metadata;
      _bytes = bytes;
      _retainedBytes = null;
      _totalWeight = metadata.totalWeight();
      _min = metadata.min();
      _max = metadata.max();
      _compression = metadata.compression();
      _numCentroids = metadata.centroidCount();
      _mainCapacity = metadata.mainCapacity();
      _bufferCapacity = metadata.bufferCapacity();
      _centroidSize = metadata.centroidSize();
      _centroidOffset = metadata.centroidOffset();
      _decoded = false;
    }

    public double getCompression() {
      return _compression;
    }

    /// Returns validated metadata without allocating centroid arrays for a digest that will remain pending.
    public SerializedTDigestMetadata getMetadata() {
      inspectMetadata();
      return _metadata;
    }

    private void inspectMetadata() {
      if (Double.isNaN(_metadata.totalWeight())) {
        _metadata = TDigestUtils.inspectSerialized(ByteBuffer.wrap(_bytes), _metadata, null, null);
        _totalWeight = _metadata.totalWeight();
      }
    }

    private boolean hasUnrepresentableBoundaryMass() {
      if (!_metadata.fractionalWeights() || _metadata.needsLegacyFallback()) {
        return false;
      }
      // Sorting unchanged unordered inputs can expose a fractional boundary that was interior on the wire.
      if (_metadata.unorderedMeans()) {
        return true;
      }
      ByteBuffer encoded = ByteBuffer.wrap(_bytes);
      encoded.position(_centroidOffset);
      double firstWeight = 0.0;
      double lastWeight = 0.0;
      int positiveCount = 0;
      for (int i = 0; i < _numCentroids; i++) {
        double weight = _centroidSize == TDigestUtils.VERBOSE_CENTROID_SIZE ? encoded.getDouble() : encoded.getFloat();
        encoded.position(encoded.position() + _centroidSize / 2);
        if (weight > 0.0) {
          if (positiveCount++ == 0) {
            firstWeight = weight;
          }
          lastWeight = weight;
        }
      }
      return positiveCount == 1 ? firstWeight < 2.0 && firstWeight != 1.0
          : positiveCount > 1 && (firstWeight < 1.0 || lastWeight < 1.0);
    }

    private byte[] retainBytes() {
      if (_retainedBytes == null) {
        _retainedBytes = _bytes.clone();
      }
      return _retainedBytes;
    }

    private void decode() {
      if (_decoded) {
        return;
      }
      int encodedCentroidCount = _numCentroids;
      ensureCapacity(Math.addExact(encodedCentroidCount, 2));
      if (Double.isNaN(_metadata.totalWeight())) {
        _metadata = TDigestUtils.inspectSerialized(ByteBuffer.wrap(_bytes), _metadata, _means, _weights);
        _totalWeight = _metadata.totalWeight();
      } else {
        // Pending state was already inspected before retaining bytes. Its later first read only materializes
        // those validated centroids; regular merged inputs compute flags and weights in the decode pass above.
        ByteBuffer input = ByteBuffer.wrap(_bytes);
        input.position(_centroidOffset);
        for (int i = 0; i < encodedCentroidCount; i++) {
          _weights[i] = _centroidSize == TDigestUtils.VERBOSE_CENTROID_SIZE ? input.getDouble() : input.getFloat();
          double mean = _centroidSize == TDigestUtils.VERBOSE_CENTROID_SIZE ? input.getDouble() : input.getFloat();
          _means[i] = clamp(mean, _min, _max);
        }
      }
      if (!_metadata.needsLegacyFallback()) {
        if (_metadata.hasZeroWeightCentroids()) {
          int nonZeroCount = 0;
          for (int i = 0; i < encodedCentroidCount; i++) {
            if (_weights[i] != 0.0) {
              _means[nonZeroCount] = _means[i];
              _weights[nonZeroCount++] = _weights[i];
            }
          }
          encodedCentroidCount = nonZeroCount;
        }
        if (_metadata.unorderedMeans()) {
          it.unimi.dsi.fastutil.Arrays.mergeSort(0, encodedCentroidCount,
              (first, second) -> Double.compare(_means[first], _means[second]),
              (first, second) -> {
                double mean = _means[first];
                _means[first] = _means[second];
                _means[second] = mean;
                double weight = _weights[first];
                _weights[first] = _weights[second];
                _weights[second] = weight;
              });
        }
        _numCentroids = normalizeBoundaries(_means, _weights, encodedCentroidCount, _min, _max);
      }
      _decoded = true;
    }

    private void ensureCapacity(int capacity) {
      if (capacity > 0 && (_means == null || capacity > _means.length)) {
        int newCapacity = _means == null ? capacity : Math.max(capacity, Math.multiplyExact(_means.length, 2));
        _means = new double[newCapacity];
        _weights = new double[newCapacity];
      }
    }
  }
}
