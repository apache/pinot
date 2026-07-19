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
import com.tdunning.math.stats.TDigest;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/// Accumulates raw values and serialized TDigests into centroids without repeatedly sorting existing centroids.
///
/// The accumulator is used while a percentile TDigest result, or one of its group-by results, is being built. Raw
/// values are sorted in small batches, while serialized TDigest centroids are decoded in their existing sorted order.
/// Both are linearly merged with the accumulated centroids and compressed using the same weight-limit rule as
/// [MergingDigest]. [#toTDigest()] normalizes the result to a standard [MergingDigest], so the public intermediate
/// and wire representations remain unchanged. Intermediate serialization retains the standard small encoding when
/// its explicit capacity is required to decode an oversized compact digest.
///
/// The accumulator implements [TDigest] so process-local combine and reduction can retain the primitive state.
/// Quantile queries and CDFs read the primitive state directly. Centroid iteration and serialization use the
/// canonical library implementation returned from [#toTDigest()].
///
/// Serialized group state keeps its first digest pending, and allocates primitive centroid buffers only when another
/// input must be merged. The raw-value buffer remains unallocated for serialized state until a raw value is added.
///
/// Instances are externally serialized per aggregation or group key and are not safe for concurrent mutation.
final class PercentileTDigestAccumulator extends TDigest {
  private static final int MIN_RAW_BUFFER_SIZE = 256;
  private static final int MAX_RAW_BUFFER_SIZE = 10_000;
  private static final int CENTROID_CAPACITY_PADDING = 10;
  private static final int VERBOSE_ENCODING = 1;
  private static final int SMALL_ENCODING = 2;
  private static final int VERBOSE_HEADER_SIZE = 32;
  private static final int VERBOSE_CENTROID_SIZE = 16;
  private static final int SMALL_HEADER_SIZE = 30;
  private static final int SMALL_CENTROID_SIZE = 8;
  private static final int DEFAULT_MERGE_BUFFER_MULTIPLIER = 5;

  private final double _compression;
  private final int _centroidCapacity;
  private double[] _rawValues;
  private double[] _centroidMeans;
  private double[] _centroidWeights;
  private double[] _outputMeans;
  private double[] _outputWeights;
  private double[] _incomingMeans;
  private double[] _incomingWeights;
  private byte[] _pendingSerializedTDigest;
  private double _pendingSerializedTotalWeight = Double.NaN;
  private boolean _hasSerializedInput;

  private int _numRawValues;
  private int _numCentroids;
  private int _serializedMainCapacity;
  private int _serializedBufferCapacity;
  private double _totalWeight;
  private double _min = Double.POSITIVE_INFINITY;
  private double _max = Double.NEGATIVE_INFINITY;

  PercentileTDigestAccumulator(int compression) {
    this((double) compression, true, true);
  }

  private PercentileTDigestAccumulator(double compression, boolean allocateRawBuffer, boolean allocateMergeBuffers) {
    if (!(compression > 0.0) || !Double.isFinite(compression)) {
      throw new IllegalArgumentException("TDigest compression must be positive: " + compression);
    }
    _compression = compression;
    long roundedCompression = (long) Math.ceil(compression);
    _centroidCapacity = getCentroidCapacity(compression);
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
      int numValues = Math.min(toExclusive - from, _rawValues.length - _numRawValues);
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
      if (_numRawValues == _rawValues.length) {
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
    if (_numRawValues == _rawValues.length) {
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
    flush();
    ensureIncomingCapacity(1);
    _incomingMeans[0] = value;
    _incomingWeights[0] = weight;
    if (mergeSorted(_incomingMeans, _incomingWeights, 1, weight)) {
      _min = Math.min(_min, value);
      _max = Math.max(_max, value);
    }
  }

  @Override
  public void add(TDigest other) {
    if (other instanceof PercentileTDigestAccumulator) {
      addAccumulator((PercentileTDigestAccumulator) other);
      return;
    }

    other.compress();
    int numCentroids = other.centroidCount();
    materializePendingSerializedTDigest();
    flush();
    ensureIncomingCapacity(numCentroids);
    int index = 0;
    double incomingWeight = 0.0;
    for (Centroid centroid : other.centroids()) {
      _incomingMeans[index] = centroid.mean();
      double weight = centroid.count();
      _incomingWeights[index] = weight;
      incomingWeight += weight;
      index++;
    }
    if (mergeSorted(_incomingMeans, _incomingWeights, index, incomingWeight)) {
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
    if (other._pendingSerializedTDigest != null) {
      addSerializedTDigest(other._pendingSerializedTDigest);
      return;
    }

    other.compress();
    materializePendingSerializedTDigest();
    flush();
    preserveSerializedCapacity(other._serializedMainCapacity, other._serializedBufferCapacity);
    if (mergeSorted(other._centroidMeans, other._centroidWeights, other._numCentroids, other._totalWeight)) {
      _min = Math.min(_min, other._min);
      _max = Math.max(_max, other._max);
    }
  }

  void addSerializedTDigest(byte[] bytes) {
    flush();
    if (!_hasSerializedInput && _numCentroids == 0 && _totalWeight == 0.0) {
      _pendingSerializedTotalWeight = getSerializedTotalWeight(bytes);
      _pendingSerializedTDigest = bytes;
      _hasSerializedInput = true;
      return;
    }
    materializePendingSerializedTDigest();
    mergeSerializedTDigest(bytes);
    _hasSerializedInput = true;
  }

  void addSerializedTDigest(SerializedTDigestInput input) {
    flush();
    if (!_hasSerializedInput && _numCentroids == 0 && _totalWeight == 0.0) {
      _pendingSerializedTDigest = input._bytes;
      _pendingSerializedTotalWeight = Double.NaN;
      _hasSerializedInput = true;
      return;
    }
    materializePendingSerializedTDigest();
    input.decode();
    mergeSerializedTDigest(input);
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
    boolean initialize = _numCentroids == 0 && _totalWeight == 0.0;
    double[] incomingMeans;
    double[] incomingWeights;
    if (encoding == VERBOSE_ENCODING) {
      double serializedCompression = input.getDouble();
      numCentroids = input.getInt();
      checkCentroidCount(numCentroids);
      checkCentroidsAvailable(input, numCentroids, VERBOSE_CENTROID_SIZE);
      checkVerboseCentroidCapacity(serializedCompression, numCentroids);
      if (initialize) {
        ensureCentroidCapacity(numCentroids);
        incomingMeans = _centroidMeans;
        incomingWeights = _centroidWeights;
      } else {
        ensureIncomingCapacity(numCentroids);
        incomingMeans = _incomingMeans;
        incomingWeights = _incomingWeights;
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
        ensureCentroidCapacity(numCentroids);
        incomingMeans = _centroidMeans;
        incomingWeights = _centroidWeights;
      } else {
        ensureIncomingCapacity(numCentroids);
        incomingMeans = _incomingMeans;
        incomingWeights = _incomingWeights;
      }
      for (int i = 0; i < numCentroids; i++) {
        incomingWeights[i] = input.getFloat();
        incomingMeans[i] = input.getFloat();
      }
    }
    preserveSerializedCapacity(mainCapacity, bufferCapacity);

    if (initialize) {
      double totalWeight = getTotalWeight(incomingWeights, numCentroids);
      if (totalWeight != 0.0) {
        _numCentroids = numCentroids;
        _totalWeight = totalWeight;
        _min = min;
        _max = max;
      }
    } else if (mergeSorted(incomingMeans, incomingWeights, numCentroids)) {
      _min = Math.min(_min, min);
      _max = Math.max(_max, max);
    }
  }

  private void mergeSerializedTDigest(SerializedTDigestInput input) {
    preserveSerializedCapacity(input._mainCapacity, input._bufferCapacity);
    boolean initialize = _numCentroids == 0 && _totalWeight == 0.0;
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
    } else if (mergeSorted(input._means, input._weights, input._numCentroids, input._totalWeight)) {
      _min = Math.min(_min, input._min);
      _max = Math.max(_max, input._max);
    }
  }

  @Override
  public void compress() {
    materializePendingSerializedTDigest();
    flush();
  }

  @Override
  public long size() {
    if (_pendingSerializedTDigest != null) {
      if (Double.isNaN(_pendingSerializedTotalWeight)) {
        _pendingSerializedTotalWeight = getSerializedTotalWeight(_pendingSerializedTDigest);
      }
      return (long) _pendingSerializedTotalWeight;
    }
    return (long) (_totalWeight + _numRawValues);
  }

  @Override
  public double cdf(double value) {
    compress();
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
    if (value <= _min) {
      return 0.0;
    }
    if (value >= _max) {
      return 1.0;
    }
    if (value <= _centroidMeans[0]) {
      double width = _centroidMeans[0] - _min;
      return width > 0.0 ? (value - _min) / width * _centroidWeights[0] / _totalWeight / 2.0 : 0.0;
    }
    int lastIndex = _numCentroids - 1;
    if (value >= _centroidMeans[lastIndex]) {
      double width = _max - _centroidMeans[lastIndex];
      return width > 0.0
          ? 1.0 - (_max - value) / width * _centroidWeights[lastIndex] / _totalWeight / 2.0 : 1.0;
    }

    double weightSoFar = _centroidWeights[0] / 2.0;
    for (int i = 0; i < lastIndex; i++) {
      if (_centroidMeans[i] == value) {
        double startWeight = weightSoFar;
        int equalIndex = i;
        while (equalIndex < lastIndex && _centroidMeans[equalIndex + 1] == value) {
          weightSoFar += _centroidWeights[equalIndex] + _centroidWeights[equalIndex + 1];
          equalIndex++;
        }
        return (startWeight + weightSoFar) / 2.0 / _totalWeight;
      }
      if (_centroidMeans[i] <= value && _centroidMeans[i + 1] > value) {
        double width = _centroidMeans[i + 1] - _centroidMeans[i];
        double halfWeight = (_centroidWeights[i] + _centroidWeights[i + 1]) / 2.0;
        return width > 0.0
            ? (weightSoFar + halfWeight * (value - _centroidMeans[i]) / width) / _totalWeight
            : weightSoFar + halfWeight / _totalWeight;
      }
      weightSoFar += (_centroidWeights[i] + _centroidWeights[i + 1]) / 2.0;
    }
    throw new IllegalStateException("Unable to compute TDigest CDF");
  }

  @Override
  public double quantile(double quantile) {
    if (quantile < 0.0 || quantile > 1.0) {
      throw new IllegalArgumentException("q should be in [0,1], got " + quantile);
    }
    compress();
    if (_numCentroids == 0) {
      return Double.NaN;
    }
    if (_numCentroids == 1) {
      return _centroidMeans[0];
    }

    double index = quantile * _totalWeight;
    if (index < _centroidWeights[0] / 2.0) {
      return _min + 2.0 * index / _centroidWeights[0] * (_centroidMeans[0] - _min);
    }
    double weightSoFar = _centroidWeights[0] / 2.0;
    int lastIndex = _numCentroids - 1;
    for (int i = 0; i < lastIndex; i++) {
      double halfWeight = (_centroidWeights[i] + _centroidWeights[i + 1]) / 2.0;
      if (weightSoFar + halfWeight > index) {
        double leftWeight = index - weightSoFar;
        double rightWeight = weightSoFar + halfWeight - index;
        return weightedAverage(_centroidMeans[i], rightWeight, _centroidMeans[i + 1], leftWeight);
      }
      weightSoFar += halfWeight;
    }
    double leftWeight = index - _totalWeight - _centroidWeights[lastIndex] / 2.0;
    double rightWeight = _centroidWeights[lastIndex] / 2.0 - leftWeight;
    return weightedAverage(_centroidMeans[lastIndex], leftWeight, _max, rightWeight);
  }

  private static double weightedAverage(double firstValue, double firstWeight, double secondValue,
      double secondWeight) {
    if (firstValue > secondValue) {
      return weightedAverage(secondValue, secondWeight, firstValue, firstWeight);
    }
    double average = (firstValue * firstWeight + secondValue * secondWeight) / (firstWeight + secondWeight);
    return Math.max(firstValue, Math.min(average, secondValue));
  }

  @Override
  public Collection<Centroid> centroids() {
    return toTDigest().centroids();
  }

  @Override
  public double compression() {
    return _compression;
  }

  @Override
  public int byteSize() {
    return toTDigest().byteSize();
  }

  @Override
  public int smallByteSize() {
    return toTDigest().smallByteSize();
  }

  @Override
  public void asBytes(ByteBuffer buffer) {
    toTDigest().asBytes(buffer);
  }

  @Override
  public void asSmallBytes(ByteBuffer buffer) {
    toTDigest().asSmallBytes(buffer);
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
  public int centroidCount() {
    if (_pendingSerializedTDigest != null) {
      return getSerializedCentroidCount(_pendingSerializedTDigest);
    }
    flush();
    normalizeCentroidCapacity();
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
      return _pendingSerializedTDigest.clone();
    }
    flush();
    if (requiresCapacityPreservingEncoding()) {
      return toCapacityPreservingBytes();
    }
    TDigest tDigest = toTDigest();
    byte[] bytes = new byte[tDigest.byteSize()];
    tDigest.asBytes(ByteBuffer.wrap(bytes));
    return bytes;
  }

  TDigest toTDigest() {
    if (_pendingSerializedTDigest != null) {
      return MergingDigest.fromBytes(ByteBuffer.wrap(_pendingSerializedTDigest));
    }
    flush();
    if (_numCentroids == 0) {
      return TDigest.createMergingDigest(_compression);
    }

    if (requiresCapacityPreservingEncoding()) {
      return toCapacityPreservingTDigest();
    }
    normalizeCentroidCapacity();
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
    buffer.flip();
    return MergingDigest.fromBytes(buffer);
  }

  private boolean requiresCapacityPreservingEncoding() {
    return _numCentroids > _centroidCapacity && _serializedMainCapacity >= _numCentroids
        && (double) (float) _compression == _compression;
  }

  private void recompressCentroids() {
    double[] means = _centroidMeans;
    double[] weights = _centroidWeights;
    int numCentroids = _numCentroids;
    double totalWeight = _totalWeight;
    _numCentroids = 0;
    _totalWeight = 0.0;
    if (!mergeSorted(means, weights, numCentroids, totalWeight)) {
      throw new IllegalStateException("Cannot recompress an empty TDigest");
    }
  }

  private void normalizeCentroidCapacity() {
    if (_numCentroids <= _centroidCapacity || _serializedMainCapacity < _numCentroids
        || (double) (float) _compression == _compression) {
      return;
    }
    recompressCentroids();
    if (_numCentroids > _centroidCapacity) {
      throw new IllegalStateException("Unable to recompress TDigest to its default centroid capacity: "
          + _numCentroids);
    }
  }

  private TDigest toCapacityPreservingTDigest() {
    return MergingDigest.fromBytes(ByteBuffer.wrap(toCapacityPreservingBytes()));
  }

  private byte[] toCapacityPreservingBytes() {
    if (_numCentroids > Short.MAX_VALUE) {
      throw new IllegalStateException("TDigest has too many centroids for capacity-preserving encoding: "
          + _numCentroids);
    }
    int mainCapacity = Math.min(Short.MAX_VALUE, Math.max(_numCentroids, _serializedMainCapacity));
    long defaultBufferCapacity = Math.multiplyExact(DEFAULT_MERGE_BUFFER_MULTIPLIER, (long) Math.ceil(_compression));
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

  private void flush() {
    if (_numRawValues == 0) {
      return;
    }

    Arrays.sort(_rawValues, 0, _numRawValues);
    int preferredOutputCapacity = getPreferredOutputCapacity(_numRawValues);
    ensureOutputCapacity(1);
    double newTotalWeight = _totalWeight + _numRawValues;
    double normalizer = _compression / (Math.PI * newTotalWeight);
    int rawIndex = 0;
    int centroidIndex = 0;
    int numOutputCentroids = 0;
    double weightSoFar = 0.0;

    while (rawIndex < _numRawValues || centroidIndex < _numCentroids) {
      double mean;
      double weight;
      if (centroidIndex == _numCentroids || (rawIndex < _numRawValues
          && _rawValues[rawIndex] <= _centroidMeans[centroidIndex])) {
        mean = _rawValues[rawIndex++];
        weight = 1.0;
      } else {
        mean = _centroidMeans[centroidIndex];
        weight = _centroidWeights[centroidIndex++];
      }
      if (numOutputCentroids == 0) {
        _outputMeans[0] = mean;
        _outputWeights[0] = weight;
        numOutputCentroids = 1;
      } else {
        int currentIndex = numOutputCentroids - 1;
        double proposedWeight = _outputWeights[currentIndex] + weight;
        double z = proposedWeight * normalizer;
        double q0 = weightSoFar / newTotalWeight;
        double q2 = (weightSoFar + proposedWeight) / newTotalWeight;
        if (z * z <= q0 * (1.0 - q0) && z * z <= q2 * (1.0 - q2)) {
          _outputWeights[currentIndex] = proposedWeight;
          _outputMeans[currentIndex] += (mean - _outputMeans[currentIndex]) * weight / proposedWeight;
        } else {
          weightSoFar += _outputWeights[currentIndex];
          ensureOutputCapacity(Math.max(preferredOutputCapacity, numOutputCentroids + 1));
          _outputMeans[numOutputCentroids] = mean;
          _outputWeights[numOutputCentroids] = weight;
          numOutputCentroids++;
        }
      }
    }
    finishMerge(numOutputCentroids, newTotalWeight);
    _numRawValues = 0;
    _min = Math.min(_min, _centroidMeans[0]);
    _max = Math.max(_max, _centroidMeans[_numCentroids - 1]);
  }

  private boolean mergeSorted(double[] incomingMeans, double[] incomingWeights, int incomingCount) {
    return mergeSorted(incomingMeans, incomingWeights, incomingCount,
        getTotalWeight(incomingWeights, incomingCount));
  }

  private boolean mergeSorted(double[] incomingMeans, double[] incomingWeights, int incomingCount,
      double incomingWeight) {
    if (incomingWeight == 0.0) {
      return false;
    }

    int preferredOutputCapacity = getPreferredOutputCapacity(incomingCount);
    ensureOutputCapacity(1);
    double newTotalWeight = _totalWeight + incomingWeight;
    double normalizer = _compression / (Math.PI * newTotalWeight);
    int incomingIndex = 0;
    int centroidIndex = 0;
    int numOutputCentroids = 0;
    double weightSoFar = 0.0;
    while (incomingIndex < incomingCount || centroidIndex < _numCentroids) {
      double mean;
      double weight;
      if (centroidIndex == _numCentroids || (incomingIndex < incomingCount
          && incomingMeans[incomingIndex] <= _centroidMeans[centroidIndex])) {
        mean = incomingMeans[incomingIndex];
        weight = incomingWeights[incomingIndex++];
      } else {
        mean = _centroidMeans[centroidIndex];
        weight = _centroidWeights[centroidIndex++];
      }
      if (numOutputCentroids == 0) {
        _outputMeans[0] = mean;
        _outputWeights[0] = weight;
        numOutputCentroids = 1;
      } else {
        int currentIndex = numOutputCentroids - 1;
        double proposedWeight = _outputWeights[currentIndex] + weight;
        double z = proposedWeight * normalizer;
        double q0 = weightSoFar / newTotalWeight;
        double q2 = (weightSoFar + proposedWeight) / newTotalWeight;
        if (z * z <= q0 * (1.0 - q0) && z * z <= q2 * (1.0 - q2)) {
          _outputWeights[currentIndex] = proposedWeight;
          _outputMeans[currentIndex] += (mean - _outputMeans[currentIndex]) * weight / proposedWeight;
        } else {
          weightSoFar += _outputWeights[currentIndex];
          ensureOutputCapacity(Math.max(preferredOutputCapacity, numOutputCentroids + 1));
          _outputMeans[numOutputCentroids] = mean;
          _outputWeights[numOutputCentroids] = weight;
          numOutputCentroids++;
        }
      }
    }
    finishMerge(numOutputCentroids, newTotalWeight);
    return true;
  }

  private void finishMerge(int numOutputCentroids, double totalWeight) {
    double[] temporary = _centroidMeans;
    _centroidMeans = _outputMeans;
    _outputMeans = temporary;
    temporary = _centroidWeights;
    _centroidWeights = _outputWeights;
    _outputWeights = temporary;
    _numCentroids = numOutputCentroids;
    _totalWeight = totalWeight;
  }

  private void ensureIncomingCapacity(int capacity) {
    if (_incomingMeans == null || capacity > _incomingMeans.length) {
      int newCapacity = Math.max(capacity, _centroidCapacity);
      _incomingMeans = new double[newCapacity];
      _incomingWeights = new double[newCapacity];
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

  private void ensureRawBuffer() {
    if (_rawValues == null) {
      _rawValues = new double[getRawBufferSize((long) Math.ceil(_compression))];
    }
  }

  private static int getRawBufferSize(long roundedCompression) {
    return (int) Math.min(MAX_RAW_BUFFER_SIZE, Math.max(MIN_RAW_BUFFER_SIZE, 2L * roundedCompression));
  }

  private static int getCentroidCapacity(double compression) {
    long roundedCompression = (long) Math.ceil(compression);
    return Math.toIntExact(
        Math.addExact(Math.multiplyExact(2L, roundedCompression), CENTROID_CAPACITY_PADDING));
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
  /// state. The input must remain thread-confined and must not outlive the aggregation call that owns it.
  static final class SerializedTDigestInput {
    private byte[] _bytes;
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

    private void decode() {
      if (_decoded) {
        return;
      }
      ensureCapacity(_numCentroids);
      ByteBuffer input = ByteBuffer.wrap(_bytes);
      input.position(_centroidOffset);
      double totalWeight = 0.0;
      if (_centroidSize == VERBOSE_CENTROID_SIZE) {
        for (int i = 0; i < _numCentroids; i++) {
          double weight = input.getDouble();
          _weights[i] = weight;
          _means[i] = input.getDouble();
          totalWeight += weight;
        }
      } else {
        for (int i = 0; i < _numCentroids; i++) {
          double weight = input.getFloat();
          _weights[i] = weight;
          _means[i] = input.getFloat();
          totalWeight += weight;
        }
      }
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
    checkSmallCentroidCapacity(getSerializedDefaultCentroidCapacity(compression), numCentroids);
  }

  private static int getSerializedDefaultCentroidCapacity(double compression) {
    int capacity = (int) (2.0 * Math.ceil(compression));
    capacity += CENTROID_CAPACITY_PADDING;
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
