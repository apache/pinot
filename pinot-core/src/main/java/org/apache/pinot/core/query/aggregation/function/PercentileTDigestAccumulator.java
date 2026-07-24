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
package org.apache.pinot.core.query.aggregation.function;

import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.ScaleFunction;
import com.tdunning.math.stats.Sort;
import com.tdunning.math.stats.TDigest;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.pinot.segment.local.utils.TDigestUtils;

/// Accumulates raw values and serialized TDigests into centroids without repeatedly sorting existing centroids.
///
/// The accumulator is used while a percentile TDigest result, or one of its group-by results, is being built. Raw
/// values are sorted in small batches, while serialized TDigest centroids are decoded in their existing sorted order.
/// Both are linearly merged with the accumulated centroids and compressed using the same K1 weight-limit rule as the
/// merging digests created by [TDigestUtils]. Pinot selects K1 explicitly because t-digest 3.3's new K2 default caused
/// a material middle-quantile accuracy regression. [#toTDigest()] normalizes the result to the library's standard
/// merging digest, so the public intermediate and wire representations remain unchanged. Intermediate serialization
/// retains the standard small encoding when its explicit capacity is required to decode an oversized compact digest.
///
/// The accumulator implements [TDigest] so process-local combine and reduction can retain the primitive state.
/// Quantile queries and CDFs read the primitive state directly. Centroid iteration uses the canonical library
/// implementation returned from [#toTDigest()], while [#byteSize()] and [#asBytes(ByteBuffer)] emit [#serialize()]
/// bytes directly so generic `TDigest` serializers preserve mixed-version-compatible state.
///
/// Serialized group state keeps its first digest pending, and allocates primitive centroid buffers only when another
/// input must be merged. The raw-value buffer remains unallocated for serialized state until a raw value is added.
///
/// Instances are externally serialized per aggregation or group key and are not safe for concurrent mutation.
final class PercentileTDigestAccumulator extends TDigest {
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
  private boolean _hasSerializedInput;

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

  PercentileTDigestAccumulator(int compression) {
    this((double) compression, true, true);
  }

  private PercentileTDigestAccumulator(double compression, boolean allocateRawBuffer, boolean allocateMergeBuffers) {
    if (!(compression > 0.0) || !Double.isFinite(compression)) {
      throw new IllegalArgumentException("TDigest compression must be positive: " + compression);
    }
    super.setScaleFunction(ScaleFunction.K_1);
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
      _centroidMeans = new double[_centroidCapacity];
      _centroidWeights = new double[_centroidCapacity];
      _outputMeans = new double[_centroidCapacity];
      _outputWeights = new double[_centroidCapacity];
    }
  }

  static PercentileTDigestAccumulator forSerializedTDigest(byte[] bytes) {
    return new PercentileTDigestAccumulator(readCompression(bytes), false, false);
  }

  static PercentileTDigestAccumulator forSerializedTDigest(SerializedTDigestInput input) {
    return new PercentileTDigestAccumulator(input._compression, false, false);
  }

  static PercentileTDigestAccumulator forSerializedTDigestWithMergeBuffers(byte[] bytes) {
    return new PercentileTDigestAccumulator(readCompression(bytes), false, true);
  }

  static PercentileTDigestAccumulator forSerializedTDigest(ByteBuffer input) {
    byte[] bytes = new byte[input.remaining()];
    input.get(bytes);
    PercentileTDigestAccumulator accumulator = forSerializedTDigest(bytes);
    accumulator.addSerializedTDigest(bytes);
    return accumulator;
  }

  static PercentileTDigestAccumulator forReduction(double compression) {
    return new PercentileTDigestAccumulator(compression, false, false);
  }

  void add(double[] values, int from, int toExclusive) {
    if (from >= toExclusive) {
      return;
    }
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
    materializePendingSerializedTDigest();
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
    if (other instanceof MergingDigest && other.size() > Integer.MAX_VALUE) {
      // Centroid.count() exposes an int even though MergingDigest stores double weights internally. Preserve large
      // pre-aggregated weights by ingesting the serialized double-precision representation instead.
      addSerializedTDigest(TDigestUtils.serialize(other));
      return;
    }

    Collection<Centroid> centroids = other.centroids();
    materializePendingSerializedTDigest();
    for (Centroid centroid : centroids) {
      bufferIncomingCentroid(centroid.mean(), centroid.count());
    }
    _min = Math.min(_min, other.getMin());
    _max = Math.max(_max, other.getMax());
  }

  @Override
  public void add(List<? extends TDigest> others) {
    for (TDigest other : others) {
      add(other);
    }
  }

  private void addAccumulator(PercentileTDigestAccumulator other) {
    if (other._pendingSerializedTDigest != null) {
      addSerializedTDigest(other._pendingSerializedTDigest);
      return;
    }

    other.compress();
    materializePendingSerializedTDigest();
    preserveSerializedCapacity(other._serializedMainCapacity, other._serializedBufferCapacity);
    bufferIncomingCentroids(other._centroidMeans, other._centroidWeights, other._numCentroids);
    _min = Math.min(_min, other._min);
    _max = Math.max(_max, other._max);
  }

  void addSerializedTDigest(byte[] bytes) {
    if (!_hasSerializedInput && _numCentroids == 0 && _totalWeight == 0.0 && _numRawValues == 0
        && _numIncomingCentroids == 0) {
      byte[] retainedBytes = bytes.clone();
      _pendingSerializedTotalWeight = getSerializedTotalWeight(retainedBytes);
      _pendingSerializedTDigest = retainedBytes;
      _hasSerializedInput = true;
      return;
    }
    materializePendingSerializedTDigest();
    if (_serializedTDigestInput == null) {
      _serializedTDigestInput = new SerializedTDigestInput();
    }
    _serializedTDigestInput.reset(bytes);
    _serializedTDigestInput.decode();
    mergeSerializedTDigest(_serializedTDigestInput);
    _hasSerializedInput = true;
  }

  void addSerializedTDigest(SerializedTDigestInput input) {
    if (!_hasSerializedInput && _numCentroids == 0 && _totalWeight == 0.0 && _numRawValues == 0
        && _numIncomingCentroids == 0) {
      _pendingSerializedTDigest = input.retainBytes();
      _pendingSerializedTotalWeight = Double.NaN;
      _hasSerializedInput = true;
      return;
    }
    materializePendingSerializedTDigest();
    input.decode();
    mergeSerializedTDigest(input);
    _hasSerializedInput = true;
  }

  void addSerializedTDigestDirect(SerializedTDigestInput input) {
    if (!_hasSerializedInput && _numCentroids == 0 && _totalWeight == 0.0 && _numRawValues == 0
        && _numIncomingCentroids == 0) {
      _pendingSerializedTDigest = input.retainBytes();
      _pendingSerializedTotalWeight = Double.NaN;
      _hasSerializedInput = true;
      return;
    }
    materializePendingSerializedTDigest();
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

  private void mergeSerializedTDigest(byte[] bytes) {
    ByteBuffer input = ByteBuffer.wrap(bytes);
    int encoding = input.getInt();
    if (encoding != VERBOSE_ENCODING && encoding != SMALL_ENCODING) {
      throw new IllegalStateException("Invalid format for serialized histogram");
    }
    double min = input.getDouble();
    double max = input.getDouble();
    int numCentroids;
    int mainCapacity = 0;
    int bufferCapacity = 0;
    boolean initialize = _numCentroids == 0 && _totalWeight == 0.0 && _numRawValues == 0
        && _numIncomingCentroids == 0;
    double[] incomingMeans;
    double[] incomingWeights;
    if (encoding == VERBOSE_ENCODING) {
      double serializedCompression = input.getDouble();
      numCentroids = input.getInt();
      checkCentroidCount(numCentroids);
      checkCentroidsAvailable(input, numCentroids, VERBOSE_CENTROID_SIZE);
      checkVerboseCentroidCapacity(serializedCompression, numCentroids);
      if (initialize) {
        ensureCentroidCapacity(Math.addExact(numCentroids, 2));
        incomingMeans = _centroidMeans;
        incomingWeights = _centroidWeights;
      } else {
        incomingMeans = new double[Math.addExact(numCentroids, 2)];
        incomingWeights = new double[Math.addExact(numCentroids, 2)];
      }
      for (int i = 0; i < numCentroids; i++) {
        incomingWeights[i] = input.getDouble();
        incomingMeans[i] = input.getDouble();
      }
    } else {
      double serializedCompression = input.getFloat();
      mainCapacity = input.getShort();
      bufferCapacity = input.getShort();
      checkSmallArrayCapacities(mainCapacity, bufferCapacity);
      mainCapacity = resolveSmallMainCapacity(serializedCompression, mainCapacity);
      numCentroids = input.getShort();
      checkCentroidCount(numCentroids);
      checkCentroidsAvailable(input, numCentroids, SMALL_CENTROID_SIZE);
      checkSmallCentroidCapacity(mainCapacity, numCentroids);
      if (initialize) {
        ensureCentroidCapacity(Math.addExact(numCentroids, 2));
        incomingMeans = _centroidMeans;
        incomingWeights = _centroidWeights;
      } else {
        incomingMeans = new double[Math.addExact(numCentroids, 2)];
        incomingWeights = new double[Math.addExact(numCentroids, 2)];
      }
      for (int i = 0; i < numCentroids; i++) {
        incomingWeights[i] = input.getFloat();
        incomingMeans[i] = input.getFloat();
      }
    }
    numCentroids = normalizeBoundaries(incomingMeans, incomingWeights, numCentroids, min, max);
    preserveSerializedCapacity(mainCapacity, bufferCapacity);

    if (initialize) {
      double totalWeight = getTotalWeight(incomingWeights, numCentroids);
      if (totalWeight != 0.0) {
        _numCentroids = numCentroids;
        _totalWeight = totalWeight;
        _min = min;
        _max = max;
      }
    } else {
      bufferIncomingCentroids(incomingMeans, incomingWeights, numCentroids);
      _min = Math.min(_min, min);
      _max = Math.max(_max, max);
    }
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
    if (_pendingSerializedTDigest != null) {
      if (Double.isNaN(_pendingSerializedTotalWeight)) {
        _pendingSerializedTotalWeight = getSerializedTotalWeight(_pendingSerializedTDigest);
      }
      return (long) _pendingSerializedTotalWeight;
    }
    return (long) (_totalWeight + _incomingWeight + _numRawValues);
  }

  @Override
  public double cdf(double value) {
    if (Double.isNaN(value) || Double.isInfinite(value)) {
      throw new IllegalArgumentException(String.format("Invalid value: %f", value));
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
      centroids.add(Centroid.createWeighted(_centroidMeans[i], (int) _centroidWeights[i], null));
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
    compress();
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
    compress();
    buffer.put(toCapacityPreservingBytes());
  }

  @Override
  public TDigest recordAllData() {
    return toTDigest().recordAllData();
  }

  @Override
  public boolean isRecording() {
    return false;
  }

  @Override
  public void setScaleFunction(ScaleFunction scaleFunction) {
    if (scaleFunction != ScaleFunction.K_1) {
      throw new UnsupportedOperationException("PercentileTDigestAccumulator only supports ScaleFunction.K_1");
    }
    super.setScaleFunction(scaleFunction);
  }

  @Override
  public int centroidCount() {
    if (_pendingSerializedTDigest != null) {
      return getSerializedCentroidCount(_pendingSerializedTDigest);
    }
    flush();
    return _numCentroids;
  }

  @Override
  public double getMin() {
    if (_pendingSerializedTDigest != null) {
      return ByteBuffer.wrap(_pendingSerializedTDigest).getDouble(Integer.BYTES);
    }
    flush();
    return _min;
  }

  @Override
  public double getMax() {
    if (_pendingSerializedTDigest != null) {
      return ByteBuffer.wrap(_pendingSerializedTDigest).getDouble(Integer.BYTES + Double.BYTES);
    }
    flush();
    return _max;
  }

  byte[] serialize() {
    if (_pendingSerializedTDigest != null) {
      getSerializedTotalWeight(_pendingSerializedTDigest);
      if (ByteBuffer.wrap(_pendingSerializedTDigest).getInt() == VERBOSE_ENCODING) {
        byte[] serialized = TDigestUtils.makeLegacyCompatible(_pendingSerializedTDigest);
        return serialized == _pendingSerializedTDigest ? serialized.clone() : serialized;
      }
      return _pendingSerializedTDigest.clone();
    }
    compress();
    return TDigestUtils.makeLegacyCompatible(toVerboseBytes());
  }

  TDigest toTDigest() {
    if (_pendingSerializedTDigest != null) {
      return TDigestUtils.deserialize(_pendingSerializedTDigest);
    }
    compress();
    if (_numCentroids == 0) {
      return TDigestUtils.createMergingDigest(_compression);
    }
    return TDigestUtils.deserialize(TDigestUtils.makeLegacyCompatible(toVerboseBytes()));
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
    int mainCapacity = Math.min(Short.MAX_VALUE, Math.max(_numCentroids, _serializedMainCapacity));
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
    // Raw aggregation and the serialized direct path already merge incrementally. Keeping twice as many centroids
    // for every small flush increases their hot-path cost without avoiding a later merge, so use the configured
    // compression there. Buffered reducer inputs retain the two-level merge for its accuracy benefit.
    flush(_numIncomingCentroids == 0 ? getIncrementalCompression() : _workingCompression);
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
    normalizeBoundaryCentroids();
    int preferredOutputCapacity = getPreferredOutputCapacity(_numRawValues);
    ensureOutputCapacity(1);
    double newTotalWeight = _totalWeight + _numRawValues;
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
      Sort.stableSort(_incomingOrder, _incomingMeans, incomingCount);
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

    normalizeBoundaryCentroids();
    int preferredOutputCapacity = getPreferredOutputCapacity(incomingCount);
    ensureOutputCapacity(1);
    double newTotalWeight = _totalWeight + incomingWeight;
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
      int newCapacity = _incomingMeans == null ? Math.max(capacity, _centroidCapacity)
          : Math.max(capacity, Math.min(_pendingCentroidCapacity,
              Math.multiplyExact(_incomingMeans.length, DEFAULT_MERGE_BUFFER_MULTIPLIER)));
      _incomingMeans = _incomingMeans == null ? new double[newCapacity] : Arrays.copyOf(_incomingMeans, newCapacity);
      _incomingWeights =
          _incomingWeights == null ? new double[newCapacity] : Arrays.copyOf(_incomingWeights, newCapacity);
    }
  }

  private void ensureIncomingSortCapacity(int capacity) {
    ensureSortedIncomingCapacity(capacity);
    if (_incomingOrder == null || capacity > _incomingOrder.length) {
      int newCapacity = _incomingOrder == null ? Math.max(capacity, _centroidCapacity)
          : Math.max(capacity, Math.multiplyExact(_incomingOrder.length, 2));
      _incomingOrder = new int[newCapacity];
    }
  }

  private void ensureSortedIncomingCapacity(int capacity) {
    if (_sortedIncomingMeans == null || capacity > _sortedIncomingMeans.length) {
      int newCapacity = _sortedIncomingMeans == null ? Math.max(capacity, _centroidCapacity)
          : Math.max(capacity, Math.multiplyExact(_sortedIncomingMeans.length, 2));
      _sortedIncomingMeans = new double[newCapacity];
      _sortedIncomingWeights = new double[newCapacity];
    }
  }

  private void bufferIncomingCentroid(double mean, double weight) {
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
      _pendingSerializedTDigest = null;
      _pendingSerializedTotalWeight = Double.NaN;
      mergeSerializedTDigest(bytes);
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

    if (_numCentroids == 1) {
      double weight = _centroidWeights[0];
      if (weight <= 1.0) {
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
      if (weight <= 1.0) {
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
    return (int) Math.min(MAX_RAW_BUFFER_SIZE, Math.max(MIN_RAW_BUFFER_SIZE, 2L * roundedCompression));
  }

  private static int getCentroidCapacity(double compression) {
    double normalizedCompression = Math.max(MIN_COMPRESSION, compression);
    int padding = normalizedCompression < 30.0 ? LOW_COMPRESSION_CAPACITY_PADDING
        : DEFAULT_CENTROID_CAPACITY_PADDING;
    return Math.toIntExact((long) Math.ceil(2.0 * normalizedCompression + padding));
  }

  private static int getPendingCentroidCapacity(int centroidCapacity) {
    return Math.min(MAX_RAW_BUFFER_SIZE,
        Math.max(MIN_RAW_BUFFER_SIZE, Math.multiplyExact(DEFAULT_MERGE_BUFFER_MULTIPLIER, centroidCapacity)));
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
      int newCapacity = Math.max(capacity, _centroidCapacity);
      _outputMeans = new double[newCapacity];
      _outputWeights = new double[newCapacity];
    } else if (capacity > _outputMeans.length) {
      int newCapacity = Math.max(capacity, Math.multiplyExact(_outputMeans.length, 2));
      _outputMeans = Arrays.copyOf(_outputMeans, newCapacity);
      _outputWeights = Arrays.copyOf(_outputWeights, newCapacity);
    }
  }

  private static double getTotalWeight(double[] weights, int count) {
    double totalWeight = 0.0;
    for (int i = 0; i < count; i++) {
      totalWeight += weights[i];
    }
    return totalWeight;
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
    if (sameSignMeans || Math.copySign(1.0, firstMean) == Math.copySign(1.0, secondMean)) {
      return firstMean + (secondMean - firstMean) * secondWeight / totalWeight;
    }
    return firstMean * (firstWeight / totalWeight) + secondMean * (secondWeight / totalWeight);
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

  private static int getSerializedCentroidCount(byte[] bytes) {
    ByteBuffer input = ByteBuffer.wrap(bytes);
    int encoding = input.getInt();
    input.getDouble();
    input.getDouble();
    int numCentroids;
    if (encoding == VERBOSE_ENCODING) {
      double compression = input.getDouble();
      numCentroids = input.getInt();
      checkCentroidCount(numCentroids);
      checkCentroidsAvailable(input, numCentroids, VERBOSE_CENTROID_SIZE);
      checkVerboseCentroidCapacity(compression, numCentroids);
    } else if (encoding == SMALL_ENCODING) {
      double compression = input.getFloat();
      int mainCapacity = input.getShort();
      int bufferCapacity = input.getShort();
      checkSmallArrayCapacities(mainCapacity, bufferCapacity);
      mainCapacity = resolveSmallMainCapacity(compression, mainCapacity);
      numCentroids = input.getShort();
      checkCentroidCount(numCentroids);
      checkCentroidsAvailable(input, numCentroids, SMALL_CENTROID_SIZE);
      checkSmallCentroidCapacity(mainCapacity, numCentroids);
    } else {
      throw new IllegalStateException("Invalid format for serialized histogram");
    }
    return numCentroids;
  }

  private static double getSerializedTotalWeight(byte[] bytes) {
    ByteBuffer input = ByteBuffer.wrap(bytes);
    int encoding = input.getInt();
    input.getDouble();
    input.getDouble();
    int numCentroids;
    int centroidSize;
    if (encoding == VERBOSE_ENCODING) {
      double compression = input.getDouble();
      numCentroids = input.getInt();
      centroidSize = VERBOSE_CENTROID_SIZE;
      checkCentroidCount(numCentroids);
      checkCentroidsAvailable(input, numCentroids, centroidSize);
      checkVerboseCentroidCapacity(compression, numCentroids);
    } else if (encoding == SMALL_ENCODING) {
      double compression = input.getFloat();
      int mainCapacity = input.getShort();
      int bufferCapacity = input.getShort();
      checkSmallArrayCapacities(mainCapacity, bufferCapacity);
      mainCapacity = resolveSmallMainCapacity(compression, mainCapacity);
      numCentroids = input.getShort();
      centroidSize = SMALL_CENTROID_SIZE;
      checkCentroidCount(numCentroids);
      checkCentroidsAvailable(input, numCentroids, centroidSize);
      checkSmallCentroidCapacity(mainCapacity, numCentroids);
    } else {
      throw new IllegalStateException("Invalid format for serialized histogram");
    }

    double totalWeight = 0.0;
    for (int i = 0; i < numCentroids; i++) {
      totalWeight += centroidSize == VERBOSE_CENTROID_SIZE ? input.getDouble() : input.getFloat();
      input.position(input.position() + centroidSize / 2);
    }
    return totalWeight;
  }

  /// Reusable, invocation-local view of one serialized TDigest for group-by-MV fanout.
  ///
  /// [#reset(byte\[\])] parses and validates the header once per input row. Centroid arrays are decoded lazily and
  /// reused across rows, so all groups for a row merge the same primitive input without sharing mutable accumulator
  /// state. A pending accumulator requests an immutable byte snapshot, created at most once per reset and shared by
  /// the row's group fanout. The input must remain thread-confined and must not outlive the aggregation call that owns
  /// it.
  static final class SerializedTDigestInput {
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

    void reset(byte[] bytes) {
      _retainedBytes = null;
      ByteBuffer input = ByteBuffer.wrap(bytes);
      int encoding = input.getInt();
      if (encoding != VERBOSE_ENCODING && encoding != SMALL_ENCODING) {
        throw new IllegalStateException("Invalid format for serialized histogram");
      }
      _bytes = bytes;
      _min = input.getDouble();
      _max = input.getDouble();
      if (encoding == VERBOSE_ENCODING) {
        _compression = input.getDouble();
        _numCentroids = input.getInt();
        _mainCapacity = 0;
        _bufferCapacity = 0;
        _centroidSize = VERBOSE_CENTROID_SIZE;
      } else {
        _compression = input.getFloat();
        _mainCapacity = input.getShort();
        _bufferCapacity = input.getShort();
        checkSmallArrayCapacities(_mainCapacity, _bufferCapacity);
        _mainCapacity = resolveSmallMainCapacity(_compression, _mainCapacity);
        _numCentroids = input.getShort();
        _centroidSize = SMALL_CENTROID_SIZE;
      }
      checkCentroidCount(_numCentroids);
      checkCentroidsAvailable(input, _numCentroids, _centroidSize);
      if (_centroidSize == VERBOSE_CENTROID_SIZE) {
        checkVerboseCentroidCapacity(_compression, _numCentroids);
      } else {
        checkSmallCentroidCapacity(_mainCapacity, _numCentroids);
      }
      _centroidOffset = input.position();
      _decoded = false;
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
      double totalWeight = 0.0;
      if (_centroidSize == VERBOSE_CENTROID_SIZE) {
        for (int i = 0; i < encodedCentroidCount; i++) {
          double weight = input.getDouble();
          _weights[i] = weight;
          _means[i] = input.getDouble();
          totalWeight += weight;
        }
      } else {
        for (int i = 0; i < encodedCentroidCount; i++) {
          double weight = input.getFloat();
          _weights[i] = weight;
          _means[i] = input.getFloat();
          totalWeight += weight;
        }
      }
      _numCentroids = normalizeBoundaries(_means, _weights, encodedCentroidCount, _min, _max);
      _totalWeight = totalWeight;
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

  private static void checkCentroidCount(int numCentroids) {
    if (numCentroids < 0) {
      throw new IllegalArgumentException("Invalid negative TDigest centroid count: " + numCentroids);
    }
  }

  private static void checkCentroidsAvailable(ByteBuffer input, int numCentroids, int centroidSize) {
    if (numCentroids > input.remaining() / centroidSize) {
      throw new BufferUnderflowException();
    }
  }

  private static void checkSmallArrayCapacities(int mainCapacity, int bufferCapacity) {
    if (mainCapacity < -1) {
      throw new NegativeArraySizeException(Integer.toString(mainCapacity));
    }
    if (bufferCapacity < -1) {
      throw new NegativeArraySizeException(Integer.toString(bufferCapacity));
    }
  }

  private static int resolveSmallMainCapacity(double compression, int mainCapacity) {
    if (mainCapacity != -1) {
      return mainCapacity;
    }
    return getSerializedDefaultCentroidCapacity(compression);
  }

  private static void checkVerboseCentroidCapacity(double compression, int numCentroids) {
    int defaultCapacity = getSerializedDefaultCentroidCapacity(compression);
    int legacyCapacity = Math.addExact(Math.multiplyExact(2, (int) Math.ceil(compression)),
        DEFAULT_CENTROID_CAPACITY_PADDING);
    checkSmallCentroidCapacity(Math.max(defaultCapacity, legacyCapacity), numCentroids);
  }

  private static int getSerializedDefaultCentroidCapacity(double compression) {
    int capacity = getCentroidCapacity(compression);
    if (capacity < 0) {
      throw new NegativeArraySizeException(Integer.toString(capacity));
    }
    return capacity;
  }

  private static void checkSmallCentroidCapacity(int mainCapacity, int numCentroids) {
    if (numCentroids > mainCapacity) {
      throw new ArrayIndexOutOfBoundsException(
          "Index " + mainCapacity + " out of bounds for length " + mainCapacity);
    }
  }
}
