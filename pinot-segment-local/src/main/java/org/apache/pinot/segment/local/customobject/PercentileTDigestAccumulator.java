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

/// Accumulates raw values and serialized TDigests into primitive centroids using Pinot's K1 weight-limit rule.
///
/// Raw values are sorted in small batches; serialized centroids retain their existing sorted order. Both are
/// linearly merged with accumulated centroids. K1 preserves Pinot's existing middle-quantile behavior, and the
/// legacy verbose and small encodings preserve stored data and mixed-version wire compatibility.
///
/// Serialized state keeps its first digest pending and allocates centroid buffers only when another input must be
/// merged or the digest is queried. Raw-value buffers remain unallocated until a raw value is added.
///
/// Instances are externally serialized per aggregation or group key and are not safe for concurrent mutation.
public final class PercentileTDigestAccumulator extends TDigest {
  private static final int MIN_RAW_BUFFER_SIZE = 256;
  private static final int MAX_RAW_BUFFER_SIZE = 10_000;
  private static final int MIN_COMPRESSION = 10;
  private static final int DEFAULT_CENTROID_CAPACITY_PADDING = 10;
  private static final int LOW_COMPRESSION_CAPACITY_PADDING = 30;
  private static final int TWO_LEVEL_COMPRESSION_MULTIPLIER = 2;
  private static final int MIN_INCREMENTAL_COMPRESSION = 50;
  private static final int VERBOSE_ENCODING = 1;
  private static final int SMALL_ENCODING = 2;
  private static final int VERBOSE_HEADER_SIZE = 32;
  private static final int VERBOSE_CENTROID_SIZE = 16;
  private static final int SMALL_HEADER_SIZE = 30;
  private static final int SMALL_CENTROID_SIZE = 8;
  private static final int DEFAULT_MERGE_BUFFER_MULTIPLIER = 5;

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
  private int[] _incomingOrder;
  private SerializedTDigestInput _serializedTDigestInput;
  private byte[] _pendingSerializedTDigest;
  private double _pendingSerializedTotalWeight = Double.NaN;
  private SerializedTDigestMetadata _pendingSerializedMetadata;
  private boolean _legacyDegraded;
  private byte[] _legacySerializedBytes;
  private double _legacySerializedTotalWeight;
  private double _legacySerializedMin;
  private double _legacySerializedMax;
  private boolean _hasSerializedInput;
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
    this((double) compression, true, true);
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
    _centroidCapacity = getCentroidCapacity(_compression);
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
    return new PercentileTDigestAccumulator(compression, false, false);
  }

  public void add(double[] values, int from, int toExclusive) {
    if (from >= toExclusive) {
      return;
    }
    materializePendingSerializedTDigest();
    if (_legacyDegraded) {
      for (int i = from; i < toExclusive; i++) {
        add(values[i]);
      }
      return;
    }
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
    materializePendingSerializedTDigest();
    if (_legacyDegraded) {
      _totalWeight += 1.0;
      _min = Math.min(_min, value);
      _max = Math.max(_max, value);
      return;
    }
    ensureRawBuffer();
    if (_numRawValues == _rawValues.length
        || _numRawValues + _numIncomingCentroids == getPendingInputLimit()) {
      flush();
    }
    _rawValues[_numRawValues++] = value;
  }

  @Override
  public void add(double value, int weight) {
    if (weight == 1) {
      add(value);
      return;
    }
    if (Double.isNaN(value)) {
      throw new IllegalArgumentException("Cannot add NaN to t-digest");
    }
    if (weight <= 0) {
      throw new IllegalArgumentException("TDigest weight must be positive: " + weight);
    }
    materializePendingSerializedTDigest();
    if (_legacyDegraded) {
      checkTotalWeight(_totalWeight + weight);
      _totalWeight += weight;
      _min = Math.min(_min, value);
      _max = Math.max(_max, value);
      return;
    }
    bufferIncomingCentroid(value, weight);
    _min = Math.min(_min, value);
    _max = Math.max(_max, value);
  }

  @Override
  public void add(TDigest other) {
    if (other instanceof PercentileTDigestAccumulator) {
      addAccumulator((PercentileTDigestAccumulator) other);
      return;
    }
    // Read the source representation directly: the compatibility serializer may reduce its centroid resolution
    // to fit an old reader's buffer, and the centroid view narrows fractional and large weights to integers.
    ByteBuffer bytes = ByteBuffer.allocate(other.byteSize());
    other.asBytes(bytes);
    bytes.flip();
    SerializedTDigestInput input = new SerializedTDigestInput();
    input.reset(bytes, false);
    addSerializedTDigest(input);
  }

  @Override
  public void add(List<? extends TDigest> others) {
    for (TDigest other : others) {
      add(other);
    }
  }

  private void addAccumulator(PercentileTDigestAccumulator other) {
    _hasFractionalWeights |= other._hasFractionalWeights;
    if (other == this) {
      addSerializedTDigest(serialize());
      return;
    }
    if (other._pendingSerializedTDigest != null) {
      SerializedTDigestInput input = new SerializedTDigestInput();
      input.reset(other._pendingSerializedTDigest, other._pendingSerializedMetadata);
      input._retainedBytes = other._pendingSerializedTDigest;
      addSerializedTDigest(input);
      return;
    }
    if (other._legacyDegraded) {
      addSerializedTDigest(other.serialize());
      return;
    }

    other.compress();
    materializePendingSerializedTDigest();
    checkTotalWeight(_totalWeight + _incomingWeight + _numRawValues + other._totalWeight);
    if (_legacyDegraded) {
      _totalWeight += other._totalWeight;
      _min = Math.min(_min, other._min);
      _max = Math.max(_max, other._max);
      return;
    }
    preserveSerializedCapacity(other._serializedMainCapacity, other._serializedBufferCapacity);
    bufferIncomingCentroids(other._centroidMeans, other._centroidWeights, other._numCentroids);
    _min = Math.min(_min, other._min);
    _max = Math.max(_max, other._max);
  }

  public void addSerializedTDigest(byte[] bytes) {
    materializePendingSerializedTDigest();
    if (_serializedTDigestInput == null) {
      _serializedTDigestInput = new SerializedTDigestInput();
    }
    _serializedTDigestInput.reset(bytes);
    addSerializedTDigest(_serializedTDigestInput);
  }

  public void addSerializedTDigest(SerializedTDigestInput input) {
    _hasFractionalWeights |= input._metadata.fractionalWeights();
    if (!_hasSerializedInput && _numCentroids == 0 && _totalWeight == 0.0 && _numRawValues == 0
        && _numIncomingCentroids == 0) {
      _pendingSerializedTDigest = input.retainBytes();
      _pendingSerializedTotalWeight = input._totalWeight;
      _pendingSerializedMetadata = input._metadata;
      _legacyDegraded = input._metadata.needsLegacyFallback();
      _hasSerializedInput = true;
      return;
    }
    materializePendingSerializedTDigest();
    checkTotalWeight(_totalWeight + _incomingWeight + _numRawValues + input._totalWeight);
    if (_legacyDegraded || input._metadata.needsLegacyFallback()) {
      mergeLegacyDegradedInput(input);
      return;
    }
    input.decode();
    mergeSerializedTDigest(input);
    _hasSerializedInput = true;
  }

  public void addSerializedTDigestDirect(SerializedTDigestInput input) {
    _hasFractionalWeights |= input._metadata.fractionalWeights();
    if (!_hasSerializedInput && _numCentroids == 0 && _totalWeight == 0.0 && _numRawValues == 0
        && _numIncomingCentroids == 0) {
      _pendingSerializedTDigest = input.retainBytes();
      _pendingSerializedTotalWeight = input._totalWeight;
      _pendingSerializedMetadata = input._metadata;
      _legacyDegraded = input._metadata.needsLegacyFallback();
      _hasSerializedInput = true;
      return;
    }
    materializePendingSerializedTDigest();
    checkTotalWeight(_totalWeight + _incomingWeight + _numRawValues + input._totalWeight);
    if (_legacyDegraded || input._metadata.needsLegacyFallback()) {
      mergeLegacyDegradedInput(input);
      return;
    }
    input.decode();
    preserveSerializedCapacity(input._mainCapacity, input._bufferCapacity);
    boolean initialize = _numCentroids == 0 && _totalWeight == 0.0 && _numRawValues == 0
        && _numIncomingCentroids == 0;
    if (initialize) {
      if (input._totalWeight != 0.0) {
        ensureCentroidCapacity(input._numCentroids);
        System.arraycopy(input._means, 0, _centroidMeans, 0, input._numCentroids);
        System.arraycopy(input._weights, 0, _centroidWeights, 0, input._numCentroids);
        _numCentroids = input._numCentroids;
        _totalWeight = input._totalWeight;
        _min = input._min;
        _max = input._max;
      }
    } else {
      flush();
      boolean runBackwards = (_mergeCount++ & 1) != 0;
      if (mergeSorted(input._means, input._weights, input._numCentroids, input._totalWeight,
          getIncrementalCompression(),
          runBackwards)) {
        _min = Math.min(_min, input._min);
        _max = Math.max(_max, input._max);
      }
    }
    _hasSerializedInput = true;
  }

  private void mergeSerializedTDigest(SerializedTDigestInput input) {
    preserveSerializedCapacity(input._mainCapacity, input._bufferCapacity);
    boolean initialize = _numCentroids == 0 && _totalWeight == 0.0 && _numRawValues == 0
        && _numIncomingCentroids == 0;
    if (initialize) {
      if (input._totalWeight != 0.0) {
        ensureCentroidCapacity(input._numCentroids);
        System.arraycopy(input._means, 0, _centroidMeans, 0, input._numCentroids);
        System.arraycopy(input._weights, 0, _centroidWeights, 0, input._numCentroids);
        _numCentroids = input._numCentroids;
        _totalWeight = input._totalWeight;
        _min = input._min;
        _max = input._max;
      }
    } else {
      bufferIncomingCentroids(input._means, input._weights, input._numCentroids);
      _min = Math.min(_min, input._min);
      _max = Math.max(_max, input._max);
    }
  }

  @Override
  public void compress() {
    materializePendingSerializedTDigest();
    if (_legacyDegraded) {
      _totalWeight += _incomingWeight + _numRawValues;
      _incomingWeight = 0.0;
      _numIncomingCentroids = 0;
      _numRawValues = 0;
      return;
    }
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
  public double getTotalWeight() {
    return _pendingSerializedTDigest != null ? _pendingSerializedTotalWeight
        : _totalWeight + _incomingWeight + _numRawValues;
  }

  /// Bounds output centroids from the actual buffered inputs without flushing or using their potentially huge mass.
  public long getCentroidCountUpperBound() {
    return _pendingSerializedTDigest != null ? (long) _pendingSerializedMetadata.centroidCount() + 2L
        : (long) _numCentroids + _numRawValues + _numIncomingCentroids + 2L;
  }

  /// Whether fractional input weights invalidate the usual bound of one centroid per unit of mass.
  public boolean hasFractionalWeights() {
    return _hasFractionalWeights;
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
        return value == _min ? 0.5 / _totalWeight
            : (1.0 + (value - _min) / width * (_centroidWeights[0] / 2.0 - 1.0)) / _totalWeight;
      }
      return 0.0;
    }
    int lastIndex = _numCentroids - 1;
    if (value > _centroidMeans[lastIndex]) {
      double width = _max - _centroidMeans[lastIndex];
      if (width > 0.0) {
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
          double leftExcludedWeight = 0.0;
          double rightExcludedWeight = 0.0;
          if (_centroidWeights[i] == 1.0) {
            if (_centroidWeights[i + 1] == 1.0) {
              return (weightSoFar + 1.0) / _totalWeight;
            }
            leftExcludedWeight = 0.5;
          } else if (_centroidWeights[i + 1] == 1.0) {
            rightExcludedWeight = 0.5;
          }
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
      return 1.0 - 0.5 / _totalWeight;
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
    if (_numCentroids == 1) {
      return _centroidMeans[0];
    }

    double index = quantile * _totalWeight;
    if (index < 1.0) {
      return _min;
    }
    if (_centroidWeights[0] > 1.0 && index < _centroidWeights[0] / 2.0) {
      return _min + (index - 1.0) / (_centroidWeights[0] / 2.0 - 1.0) * (_centroidMeans[0] - _min);
    }
    int lastIndex = _numCentroids - 1;
    if (index > _totalWeight - 1.0) {
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
        double leftUnitWeight = 0.0;
        if (_centroidWeights[i] == 1.0) {
          if (index - weightSoFar < 0.5) {
            return _centroidMeans[i];
          }
          leftUnitWeight = 0.5;
        }
        double rightUnitWeight = 0.0;
        if (_centroidWeights[i + 1] == 1.0) {
          if (weightSoFar + halfWeight - index <= 0.5) {
            return _centroidMeans[i + 1];
          }
          rightUnitWeight = 0.5;
        }
        double leftWeight = index - weightSoFar - leftUnitWeight;
        double rightWeight = weightSoFar + halfWeight - index - rightUnitWeight;
        return weightedAverage(_centroidMeans[i], rightWeight, _centroidMeans[i + 1], leftWeight);
      }
      weightSoFar += halfWeight;
    }
    double weightToMax = index - _totalWeight - _centroidWeights[lastIndex] / 2.0;
    double weightFromLastCentroid = _centroidWeights[lastIndex] / 2.0 - weightToMax;
    return weightedAverage(_centroidMeans[lastIndex], weightToMax, _max, weightFromLastCentroid);
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
    compress();
    List<Centroid> centroids = new ArrayList<>(_numCentroids);
    // `Centroid` holds the weight as an `int`. Narrow it the same way `MergingDigest.centroids()` does, which
    // saturates at `Integer.MAX_VALUE`, instead of throwing on digests whose centroid weight exceeds it.
    for (int i = 0; i < _numCentroids; i++) {
      centroids.add(new Centroid(_centroidMeans[i], (int) _centroidWeights[i]));
    }
    return centroids;
  }

  @Override
  public double compression() {
    return _compression;
  }

  /// Returns the length of the [#serialize()] bytes rather than the raw verbose byte size, so generic `TDigest`
  /// serializers ([#byteSize()] followed by [#asBytes(ByteBuffer)]) emit the mixed-version-compatible encoding.
  /// Emitting raw verbose bytes here could produce a digest with more centroids than an old reader of the same
  /// compression allocates, which it rejects with [ArrayIndexOutOfBoundsException].
  @Override
  public int byteSize() {
    return serialize().length;
  }

  @Override
  public int smallByteSize() {
    if (_legacyDegraded) {
      return serialize().length;
    }
    compress();
    normalizeBoundaryCentroids();
    return Math.addExact(SMALL_HEADER_SIZE, Math.multiplyExact(SMALL_CENTROID_SIZE, _numCentroids));
  }

  /// Writes the [#serialize()] bytes; see [#byteSize()]. Bytes are always written in big-endian order (the t-digest
  /// wire order) regardless of the destination buffer's byte order, unlike the library's `asBytes`.
  @Override
  public void asBytes(ByteBuffer buffer) {
    buffer.put(serialize());
  }

  @Override
  public void asSmallBytes(ByteBuffer buffer) {
    if (_legacyDegraded) {
      buffer.put(serialize());
      return;
    }
    compress();
    normalizeBoundaryCentroids();
    buffer.put(toCapacityPreservingBytes());
  }

  @Override
  public int centroidCount() {
    if (_pendingSerializedTDigest != null) {
      return _pendingSerializedMetadata.centroidCount();
    }
    flush();
    return _numCentroids;
  }

  @Override
  public double getMin() {
    if (_pendingSerializedTDigest != null) {
      return _pendingSerializedMetadata.min();
    }
    flush();
    return _min;
  }

  @Override
  public double getMax() {
    if (_pendingSerializedTDigest != null) {
      return _pendingSerializedMetadata.max();
    }
    flush();
    return _max;
  }

  public byte[] serialize() {
    if (_legacyDegraded) {
      if (_pendingSerializedTDigest != null) {
        return _pendingSerializedTDigest.clone();
      }
      if (_totalWeight == _legacySerializedTotalWeight && _min == _legacySerializedMin
          && _max == _legacySerializedMax) {
        return _legacySerializedBytes.clone();
      }
      // A merge with a historical invalid distribution remains unknown. Keep that explicit in the legacy
      // encoding instead of passing its invalid centroids into the normal sorted K1 merge.
      return toLegacyDegradedBytes();
    }
    if (_pendingSerializedTDigest != null) {
      if (hasWeightedBoundaryCentroids(_pendingSerializedTDigest, _pendingSerializedMetadata.centroidCount())) {
        materializePendingSerializedTDigest();
      } else {
        if (ByteBuffer.wrap(_pendingSerializedTDigest).getInt() == VERBOSE_ENCODING) {
          byte[] serialized = TDigestUtils.makeLegacyCompatible(_pendingSerializedTDigest, _pendingSerializedMetadata);
          return serialized == _pendingSerializedTDigest ? serialized.clone() : serialized;
        }
        return _pendingSerializedTDigest.clone();
      }
    }
    compress();
    normalizeBoundaryCentroids();
    return TDigestUtils.makeLegacyCompatible(toVerboseBytes());
  }

  private static boolean hasWeightedBoundaryCentroids(byte[] bytes, int count) {
    if (count == 0) {
      return false;
    }
    ByteBuffer encoded = ByteBuffer.wrap(bytes);
    boolean verbose = encoded.getInt() == VERBOSE_ENCODING;
    double firstWeight = verbose ? encoded.getDouble(VERBOSE_HEADER_SIZE) : encoded.getFloat(SMALL_HEADER_SIZE);
    // A total mass below two cannot have two unit endpoints without inventing weight. Preserve these bytes,
    // matching the legacy fractional-weight representation, rather than repeatedly recompressing them.
    if (count == 1 && firstWeight < 2.0) {
      return false;
    }
    if (verbose) {
      return firstWeight > 1.0
          || encoded.getDouble(VERBOSE_HEADER_SIZE + VERBOSE_CENTROID_SIZE * (count - 1)) > 1.0;
    }
    return encoded.getFloat(SMALL_HEADER_SIZE) > 1.0
        || encoded.getFloat(SMALL_HEADER_SIZE + SMALL_CENTROID_SIZE * (count - 1)) > 1.0;
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

  private byte[] toVerboseBytes() {
    ByteBuffer buffer = ByteBuffer.allocate(VERBOSE_HEADER_SIZE + VERBOSE_CENTROID_SIZE * _numCentroids);
    buffer.putInt(VERBOSE_ENCODING);
    buffer.putDouble(_min);
    buffer.putDouble(_max);
    buffer.putDouble(_compression);
    buffer.putInt(_numCentroids);
    for (int i = 0; i < _numCentroids; i++) {
      buffer.putDouble(_centroidWeights[i]);
      buffer.putDouble(_centroidMeans[i]);
    }
    return buffer.array();
  }

  private byte[] toCapacityPreservingBytes() {
    checkCapacityPreservingCentroidCount();
    int mainCapacity = Math.min(Short.MAX_VALUE,
        Math.max(_centroidCapacity, Math.max(_numCentroids, _serializedMainCapacity)));
    long defaultBufferCapacity = Math.multiplyExact(DEFAULT_MERGE_BUFFER_MULTIPLIER, (long) mainCapacity);
    int bufferCapacity = Math.toIntExact(Math.min(Short.MAX_VALUE,
        Math.max(Math.max((long) _serializedBufferCapacity, mainCapacity + 1L), defaultBufferCapacity)));
    ByteBuffer buffer = ByteBuffer.allocate(SMALL_HEADER_SIZE + SMALL_CENTROID_SIZE * _numCentroids);
    buffer.putInt(SMALL_ENCODING);
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

    double newTotalWeight = _totalWeight + _numRawValues;
    checkTotalWeight(newTotalWeight);
    Arrays.sort(_rawValues, 0, _numRawValues);
    normalizeBoundaryCentroids();
    int preferredOutputCapacity = getPreferredOutputCapacity(_numRawValues);
    ensureOutputCapacity(1);
    double totalWeightNormalizer = 1.0 / newTotalWeight;
    double weightNormalizer = totalWeightNormalizer / getK1MaxWeightScale(compression);
    boolean runBackwards = (_mergeCount++ & 1) != 0;
    int rawIndex = runBackwards ? _numRawValues - 1 : 0;
    int centroidIndex = runBackwards ? _numCentroids - 1 : 0;
    int rawLimit = runBackwards ? -1 : _numRawValues;
    int centroidLimit = runBackwards ? -1 : _numCentroids;
    int direction = runBackwards ? -1 : 1;
    int numInputs = _numRawValues + _numCentroids;
    int inputIndex = 0;
    int numOutputCentroids = 0;
    double weightSoFar = 0.0;
    double firstInputMean = _numCentroids == 0 ? _rawValues[0]
        : Math.min(_rawValues[0], _centroidMeans[0]);
    double lastInputMean = _numCentroids == 0 ? _rawValues[_numRawValues - 1]
        : Math.max(_rawValues[_numRawValues - 1], _centroidMeans[_numCentroids - 1]);
    boolean allFiniteMeans = Double.isFinite(firstInputMean) && Double.isFinite(lastInputMean);
    boolean sameSignMeans = firstInputMean > 0.0 || lastInputMean < 0.0;

    while (rawIndex != rawLimit || centroidIndex != centroidLimit) {
      double mean;
      double weight;
      boolean takeRaw = centroidIndex == centroidLimit || (rawIndex != rawLimit
          && (runBackwards ? _rawValues[rawIndex] > _centroidMeans[centroidIndex]
              : _rawValues[rawIndex] <= _centroidMeans[centroidIndex]));
      if (takeRaw) {
        mean = _rawValues[rawIndex];
        weight = 1.0;
        rawIndex += direction;
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
              mergeMeans(_outputMeans[currentIndex], proposedWeight - weight, mean, weight, proposedWeight,
                  sameSignMeans);
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
    double rawMin = _rawValues[0];
    double rawMax = _rawValues[_numRawValues - 1];
    finishMerge(numOutputCentroids, newTotalWeight, compression);
    _numRawValues = 0;
    _min = Math.min(_min, rawMin);
    _max = Math.max(_max, rawMax);
  }

  private void flushIncoming(double compression) {
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
      ensureIncomingSortCapacity(incomingCount);
      stableSortIncoming(incomingCount);
      for (int i = 0; i < incomingCount; i++) {
        int sourceIndex = _incomingOrder[i];
        _sortedIncomingMeans[i] = _incomingMeans[sourceIndex];
        _sortedIncomingWeights[i] = _incomingWeights[sourceIndex];
      }
      incomingMeans = _sortedIncomingMeans;
      incomingWeights = _sortedIncomingWeights;
    }
    _numIncomingCentroids = 0;
    _incomingWeight = 0.0;
    _incomingCentroidsSorted = true;
    boolean runBackwards = (_mergeCount++ & 1) != 0;
    if (!mergeSorted(incomingMeans, incomingWeights, incomingCount, incomingWeight, compression, runBackwards)) {
      throw new IllegalStateException("Cannot flush an empty TDigest input buffer");
    }
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
        weight = incomingWeights[incomingIndex];
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
              mergeMeans(_outputMeans[currentIndex], proposedWeight - weight, mean, weight, proposedWeight,
                  sameSignMeans);
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
              (long) _incomingMeans.length * DEFAULT_MERGE_BUFFER_MULTIPLIER));
      _incomingMeans = _incomingMeans == null ? new double[newCapacity] : Arrays.copyOf(_incomingMeans, newCapacity);
      _incomingWeights =
          _incomingWeights == null ? new double[newCapacity] : Arrays.copyOf(_incomingWeights, newCapacity);
    }
  }

  private void ensureIncomingSortCapacity(int capacity) {
    ensureSortedIncomingCapacity(capacity);
    if (_incomingOrder == null || capacity > _incomingOrder.length) {
      int newCapacity = _incomingOrder == null
          ? Math.max(capacity, Math.min(_centroidCapacity, _pendingCentroidCapacity))
          : Math.max(capacity, Math.multiplyExact(_incomingOrder.length, 2));
      _incomingOrder = new int[newCapacity];
    }
  }

  private void stableSortIncoming(int count) {
    for (int i = 0; i < count; i++) {
      _incomingOrder[i] = i;
    }
    it.unimi.dsi.fastutil.Arrays.mergeSort(0, count, (left, right) -> {
      double first = _incomingMeans[_incomingOrder[left]];
      double second = _incomingMeans[_incomingOrder[right]];
      return first == second ? 0 : Double.compare(first, second);
    }, (first, second) -> {
      int index = _incomingOrder[first];
      _incomingOrder[first] = _incomingOrder[second];
      _incomingOrder[second] = index;
    });
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
    if (_pendingSerializedTDigest != null) {
      byte[] bytes = _pendingSerializedTDigest;
      SerializedTDigestMetadata metadata = _pendingSerializedMetadata;
      _pendingSerializedTDigest = null;
      _pendingSerializedMetadata = null;
      _pendingSerializedTotalWeight = Double.NaN;
      if (_serializedTDigestInput == null) {
        _serializedTDigestInput = new SerializedTDigestInput();
      }
      _serializedTDigestInput.reset(bytes, metadata);
      _serializedTDigestInput._retainedBytes = bytes;
      if (_legacyDegraded) {
        mergeLegacyDegradedInput(_serializedTDigestInput);
      } else {
        _serializedTDigestInput.decode();
        mergeSerializedTDigest(_serializedTDigestInput);
      }
    }
  }

  private void mergeLegacyDegradedInput(SerializedTDigestInput input) {
    if (_legacySerializedBytes == null && input.needsLegacyFallback()) {
      _legacySerializedBytes = input.retainBytes();
      _legacySerializedTotalWeight = input._totalWeight;
      _legacySerializedMin = input._min;
      _legacySerializedMax = input._max;
    }
    _legacyDegraded = true;
    for (int i = 0; i < _numRawValues; i++) {
      _min = Math.min(_min, _rawValues[i]);
      _max = Math.max(_max, _rawValues[i]);
    }
    _totalWeight += _incomingWeight + _numRawValues + input._totalWeight;
    _incomingWeight = 0.0;
    _numIncomingCentroids = 0;
    _numRawValues = 0;
    _min = Math.min(_min, input._min);
    _max = Math.max(_max, input._max);
    _hasSerializedInput = true;
  }

  private byte[] toLegacyDegradedBytes() {
    ByteBuffer encoded = ByteBuffer.allocate(VERBOSE_HEADER_SIZE + VERBOSE_CENTROID_SIZE);
    encoded.putInt(VERBOSE_ENCODING);
    encoded.putDouble(_min);
    encoded.putDouble(_max);
    encoded.putDouble(_compression);
    encoded.putInt(1);
    encoded.putDouble(_totalWeight + _incomingWeight + _numRawValues);
    encoded.putDouble(Double.NaN);
    return encoded.array();
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

    if (_numCentroids == 1) {
      double weight = _centroidWeights[0];
      if (weight < 2.0) {
        return;
      }
      double mean = _centroidMeans[0];
      _centroidMeans[0] = _min;
      _centroidWeights[0] = 1.0;
      if (weight == 2.0) {
        _centroidMeans[1] = _max;
        _centroidWeights[1] = 1.0;
        _numCentroids = 2;
      } else {
        _centroidMeans[1] = residualMean(mean, weight, _min, _max, weight - 2.0, _min, _max);
        _centroidWeights[1] = weight - 2.0;
        _centroidMeans[2] = _max;
        _centroidWeights[2] = 1.0;
        _numCentroids = 3;
      }
      return;
    }

    if (_centroidWeights[0] > 1.0) {
      System.arraycopy(_centroidMeans, 1, _centroidMeans, 2, _numCentroids - 1);
      System.arraycopy(_centroidWeights, 1, _centroidWeights, 2, _numCentroids - 1);
      double weight = _centroidWeights[0];
      double mean = _centroidMeans[0];
      _centroidMeans[0] = _min;
      _centroidWeights[0] = 1.0;
      _centroidMeans[1] = residualMean(mean, weight, _min, 0.0, weight - 1.0, _min, _centroidMeans[2]);
      _centroidWeights[1] = weight - 1.0;
      _numCentroids++;
    }
    int lastIndex = _numCentroids - 1;
    if (_centroidWeights[lastIndex] > 1.0) {
      double weight = _centroidWeights[lastIndex];
      double mean = _centroidMeans[lastIndex];
      _centroidMeans[lastIndex] = residualMean(mean, weight, _max, 0.0, weight - 1.0,
          _centroidMeans[lastIndex - 1], _max);
      _centroidWeights[lastIndex] = weight - 1.0;
      _centroidMeans[_numCentroids] = _max;
      _centroidWeights[_numCentroids] = 1.0;
      _numCentroids++;
    }
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

  private static int getCentroidCapacity(double compression) {
    double normalizedCompression = Math.max(MIN_COMPRESSION, compression);
    int padding = normalizedCompression < 30.0 ? LOW_COMPRESSION_CAPACITY_PADDING
        : DEFAULT_CENTROID_CAPACITY_PADDING;
    return (int) Math.min(Integer.MAX_VALUE - 8.0, Math.ceil(2.0 * normalizedCompression + padding));
  }

  private static int getPendingCentroidCapacity(int centroidCapacity) {
    return (int) Math.min(MAX_RAW_BUFFER_SIZE,
        Math.max(MIN_RAW_BUFFER_SIZE, (long) DEFAULT_MERGE_BUFFER_MULTIPLIER * centroidCapacity));
  }

  private int getPendingInputLimit() {
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

  private static double mergeMeans(double firstMean, double firstWeight, double secondMean, double secondWeight,
      double totalWeight, boolean sameSignMeans) {
    if (firstMean == secondMean) {
      return firstMean;
    }
    double average = sameSignMeans || Math.copySign(1.0, firstMean) == Math.copySign(1.0, secondMean)
        ? firstMean + (secondMean - firstMean) * (secondWeight / totalWeight)
        : firstMean * (firstWeight / totalWeight) + secondMean * (secondWeight / totalWeight);
    return clamp(average, Math.min(firstMean, secondMean), Math.max(firstMean, secondMean));
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
    ByteBuffer input = ByteBuffer.wrap(bytes);
    int encoding = input.getInt();
    if (encoding == VERBOSE_ENCODING) {
      input.getDouble();
      input.getDouble();
      return input.getDouble();
    }
    if (encoding == SMALL_ENCODING) {
      input.getDouble();
      input.getDouble();
      return input.getFloat();
    }
    throw new IllegalStateException("Invalid format for serialized histogram");
  }

  /// Reusable, invocation-local view of one serialized TDigest for group-by-MV fanout.
  ///
  /// [#reset(byte\[\])] validates the header and centroid values once per input row. Centroid arrays are decoded
  /// lazily and reused across rows, so all groups for a row merge the same primitive input without sharing mutable
  /// accumulator state. A pending accumulator requests an immutable byte snapshot, created at most once per reset
  /// and shared by the row's group fanout. The input must remain thread-confined and must not outlive the aggregation
  /// call that owns it.
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
      reset(bytes, TDigestUtils.inspectSerialized(ByteBuffer.wrap(bytes)));
    }

    /// Owns one snapshot of the encoded digest and advances the source buffer past it.
    public void reset(ByteBuffer bytes) {
      reset(bytes, true);
    }

    /// Reads a source digest, optionally enforcing the historical decoder's declared centroid capacity.
    public void reset(ByteBuffer bytes, boolean checkCapacity) {
      SerializedTDigestMetadata metadata = TDigestUtils.inspectSerialized(bytes, checkCapacity);
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

    public int getEncodedLength() {
      return _metadata.encodedLength();
    }

    public boolean hasNonFiniteMeans() {
      return _metadata.hasNonFiniteMeans();
    }

    public boolean needsLegacyFallback() {
      return _metadata.needsLegacyFallback();
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
      ByteBuffer input = ByteBuffer.wrap(_bytes);
      input.position(_centroidOffset);
      if (_centroidSize == VERBOSE_CENTROID_SIZE) {
        for (int i = 0; i < encodedCentroidCount; i++) {
          double weight = input.getDouble();
          _weights[i] = weight;
          _means[i] = input.getDouble();
        }
      } else {
        for (int i = 0; i < encodedCentroidCount; i++) {
          double weight = input.getFloat();
          _weights[i] = weight;
          // The compact encoding narrows means but retains double extrema. Its endpoint rounding can fall
          // slightly outside those extrema, so restore the validated header bounds before emitting doubles.
          _means[i] = clamp(input.getFloat(), _min, _max);
        }
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
