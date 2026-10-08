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
package org.apache.pinot.segment.local.aggregator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.local.customobject.PercentileTDigestAccumulator;
import org.apache.pinot.segment.local.customobject.PercentileTDigestAccumulator.SerializedTDigestInput;
import org.apache.pinot.segment.local.customobject.TDigest;
import org.apache.pinot.segment.local.customobject.TDigest.Centroid;
import org.apache.pinot.segment.local.utils.TDigestUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class PercentileTDigestValueAggregator implements ValueAggregator<Object, TDigest> {
  public static final DataType AGGREGATED_VALUE_TYPE = DataType.BYTES;

  // TODO: This is copied from PercentileTDigestAggregationFunction.
  public static final int DEFAULT_TDIGEST_COMPRESSION = 100;
  private static final int MIN_COMPRESSION = 10;
  private static final int DEFAULT_CENTROID_CAPACITY_PADDING = 10;
  private static final int LOW_COMPRESSION_CAPACITY_PADDING = 30;
  private static final int VERBOSE_HEADER_SIZE = Integer.BYTES + 3 * Double.BYTES + Integer.BYTES;
  private static final int VERBOSE_CENTROID_SIZE = 2 * Double.BYTES;
  private final int _compressionFactor;
  private int _maxByteSize;
  private ByteBuffer _serializationBuffer;

  public PercentileTDigestValueAggregator(List<ExpressionContext> arguments) {
    if (!arguments.isEmpty()) {
      _compressionFactor = arguments.get(0).getLiteral().getIntValue();
    } else {
      _compressionFactor = DEFAULT_TDIGEST_COMPRESSION;
    }
  }

  @Override
  public AggregationFunctionType getAggregationType() {
    return AggregationFunctionType.PERCENTILETDIGEST;
  }

  @Override
  public DataType getAggregatedValueType() {
    return AGGREGATED_VALUE_TYPE;
  }

  @Override
  public TDigest getInitialAggregatedValue(Object rawValue) {
    // NOTE: rawValue cannot be null because this aggregator can only be used for star-tree index.
    assert rawValue != null;
    TDigest initialValue;
    if (rawValue instanceof byte[]) {
      byte[] bytes = (byte[]) rawValue;
      initialValue = deserializeAggregatedValue(bytes);
    } else {
      initialValue = new NonFiniteAwareTDigest(_compressionFactor);
      addToDigest(initialValue, rawValue);
    }
    updateMaxByteSize(initialValue);
    return initialValue;
  }

  @Override
  public TDigest applyRawValue(TDigest value, Object rawValue) {
    value = asNonFiniteAware(value);
    if (rawValue instanceof byte[]) {
      value.add(deserializeAggregatedValue((byte[]) rawValue));
    } else {
      addToDigest(value, rawValue);
    }
    updateMaxByteSize(value);
    return value;
  }

  /// Adds a raw value (single value or multi-value array) to the TDigest.
  protected void addToDigest(TDigest digest, Object rawValue) {
    if (rawValue instanceof Object[]) {
      Object[] values = (Object[]) rawValue;
      for (Object value : values) {
        digest.add(ValueAggregatorUtils.toDouble(value));
      }
    } else {
      digest.add(ValueAggregatorUtils.toDouble(rawValue));
    }
  }

  @Override
  public TDigest applyAggregatedValue(TDigest value, TDigest aggregatedValue) {
    value = asNonFiniteAware(value);
    value.add(aggregatedValue);
    updateMaxByteSize(value);
    return value;
  }

  @Override
  public TDigest cloneAggregatedValue(TDigest value) {
    return asNonFiniteAware(value).copy();
  }

  @Override
  public boolean isAggregatedValueFixedSize() {
    return false;
  }

  @Override
  public int getMaxAggregatedValueByteSize() {
    return _maxByteSize;
  }

  @Override
  public byte[] serializeAggregatedValue(TDigest value) {
    int requiredCapacity = Math.max(_maxByteSize, VERBOSE_HEADER_SIZE);
    if (_serializationBuffer == null || _serializationBuffer.capacity() < requiredCapacity) {
      _serializationBuffer = ByteBuffer.allocate(requiredCapacity);
    }
    byte[] bytes = asNonFiniteAware(value).serialize(_serializationBuffer);
    _maxByteSize = Math.max(_maxByteSize, bytes.length);
    return bytes;
  }

  @Override
  public TDigest deserializeAggregatedValue(byte[] bytes) {
    _maxByteSize = Math.max(_maxByteSize, bytes.length);
    return NonFiniteAwareTDigest.fromBytes(bytes);
  }

  private static NonFiniteAwareTDigest asNonFiniteAware(TDigest value) {
    if (value instanceof NonFiniteAwareTDigest) {
      return (NonFiniteAwareTDigest) value;
    }
    NonFiniteAwareTDigest wrapper = new NonFiniteAwareTDigest(value.compression());
    wrapper.add(value);
    return wrapper;
  }

  private void updateMaxByteSize(TDigest value) {
    long defaultCapacity = getDefaultCentroidCapacity(value.compression());
    long maxCentroids = (long) Math.min(Math.ceil(value.getTotalWeight()), defaultCapacity);
    if (value instanceof NonFiniteAwareTDigest) {
      TDigest finite = ((NonFiniteAwareTDigest) value)._finiteDigest;
      if (finite instanceof PercentileTDigestAccumulator) {
        PercentileTDigestAccumulator accumulator = (PercentileTDigestAccumulator) finite;
        NonFiniteAwareTDigest wrapped = (NonFiniteAwareTDigest) value;
        boolean fractionalMass = accumulator.hasFractionalWeights()
            || wrapped._negativeInfinityWeight != Math.rint(wrapped._negativeInfinityWeight)
            || wrapped._positiveInfinityWeight != Math.rint(wrapped._positiveInfinityWeight);
        long bufferedBound = accumulator.getCentroidCountUpperBound() + 6L;
        maxCentroids = fractionalMass ? bufferedBound : Math.min(maxCentroids, bufferedBound);
      }
    }
    _maxByteSize = Math.max(_maxByteSize, getMaxVerboseByteSize(maxCentroids));
  }

  private static int getMaxVerboseByteSize(long centroidCount) {
    long maxCentroids = Math.max(centroidCount, 0L);
    return Math.toIntExact(Math.addExact(VERBOSE_HEADER_SIZE,
        Math.multiplyExact(VERBOSE_CENTROID_SIZE, maxCentroids)));
  }

  private static long getDefaultCentroidCapacity(double compression) {
    double normalizedCompression = Math.max(MIN_COMPRESSION, compression);
    int padding = normalizedCompression < 30.0 ? LOW_COMPRESSION_CAPACITY_PADDING
        : DEFAULT_CENTROID_CAPACITY_PADDING;
    long defaultCapacity = (long) Math.ceil(2.0 * normalizedCompression + padding);
    long legacyCapacity = (long) Math.ceil(2.0 * compression + DEFAULT_CENTROID_CAPACITY_PADDING);
    return Math.max(defaultCapacity, legacyCapacity);
  }

  /// Keeps non-finite values as exact tail masses while delegating all finite values to the standard implementation.
  ///
  /// The wrapper emits the standard `MergingDigest` wire format. It is mutable, externally synchronized by the
  /// segment-creation pipeline, and not safe for concurrent mutation.
  private static final class NonFiniteAwareTDigest extends TDigest {
    private static final int VERBOSE_ENCODING = 1;
    private static final int SMALL_ENCODING = 2;
    private static final int VERBOSE_HEADER_SIZE = 32;
    private static final int VERBOSE_CENTROID_SIZE = 16;

    private final TDigest _finiteDigest;
    private double _negativeInfinityWeight;
    private double _positiveInfinityWeight;
    private byte[] _finiteSerializedBytes;
    private List<Centroid> _finiteCentroids;
    private byte[] _serializedBytes;

    private NonFiniteAwareTDigest(double compression) {
      this(TDigestUtils.createMergingDigest(compression), 0L, 0L);
    }

    private NonFiniteAwareTDigest(TDigest finiteDigest, double negativeInfinityWeight, double positiveInfinityWeight) {
      _finiteDigest = finiteDigest;
      _negativeInfinityWeight = negativeInfinityWeight;
      _positiveInfinityWeight = positiveInfinityWeight;
    }

    private NonFiniteAwareTDigest copy() {
      invalidateCaches();
      TDigest finiteDigest = TDigestUtils.createMergingDigest(compression());
      if (_finiteDigest.getTotalWeight() > 0.0) {
        finiteDigest.add(List.of(_finiteDigest));
      }
      return new NonFiniteAwareTDigest(finiteDigest, _negativeInfinityWeight, _positiveInfinityWeight);
    }

    private static NonFiniteAwareTDigest fromBytes(byte[] bytes) {
      SerializedTDigestInput validated = new SerializedTDigestInput();
      validated.reset(bytes);
      if (!validated.hasNonFiniteMeans() || validated.needsLegacyFallback()) {
        return new NonFiniteAwareTDigest(TDigestUtils.deserializeFinite(validated), 0.0, 0.0);
      }
      ByteBuffer input = ByteBuffer.wrap(bytes);
      int encoding = input.getInt();
      double encodedMin = input.getDouble();
      double encodedMax = input.getDouble();
      double compression;
      int centroidCount;
      boolean verbose;
      if (encoding == VERBOSE_ENCODING) {
        compression = input.getDouble();
        centroidCount = input.getInt();
        verbose = true;
      } else if (encoding == SMALL_ENCODING) {
        compression = input.getFloat();
        input.getShort();
        input.getShort();
        centroidCount = input.getShort();
        verbose = false;
      } else {
        throw new IllegalStateException("Invalid format for serialized histogram");
      }

      int centroidOffset = input.position();
      double negativeInfinityWeight = 0L;
      double positiveInfinityWeight = 0L;
      for (int i = 0; i < centroidCount; i++) {
        double weight = verbose ? input.getDouble() : input.getFloat();
        double mean = verbose ? input.getDouble() : input.getFloat();
        if (mean == Double.NEGATIVE_INFINITY) {
          negativeInfinityWeight = negativeInfinityWeight + weight;
        } else if (mean == Double.POSITIVE_INFINITY) {
          positiveInfinityWeight = positiveInfinityWeight + weight;
        } else if (Double.isNaN(mean)) {
          throw new IllegalArgumentException("Cannot deserialize a TDigest with a NaN centroid mean");
        }
      }
      double[] finiteMeans = new double[centroidCount];
      double[] finiteWeights = new double[centroidCount];
      int finiteCount = 0;
      double finiteMin = Double.POSITIVE_INFINITY;
      double finiteMax = Double.NEGATIVE_INFINITY;
      input.position(centroidOffset);
      for (int i = 0; i < centroidCount; i++) {
        double weight = verbose ? input.getDouble() : input.getFloat();
        double mean = verbose ? input.getDouble() : input.getFloat();
        if (Double.isFinite(mean)) {
          if (!verbose) {
            mean = Math.max(encodedMin, Math.min(mean, encodedMax));
          }
          finiteWeights[finiteCount] = weight;
          finiteMeans[finiteCount] = mean;
          finiteMin = Math.min(finiteMin, mean);
          finiteMax = Math.max(finiteMax, mean);
          finiteCount++;
        }
      }

      TDigest finiteDigest;
      if (finiteCount == 0) {
        finiteDigest = TDigestUtils.createMergingDigest(compression);
      } else {
        finiteMin = Double.isFinite(encodedMin) ? encodedMin : finiteMin;
        finiteMax = Double.isFinite(encodedMax) ? encodedMax : finiteMax;
        byte[] finiteBytes = toVerboseBytes(finiteMin, finiteMax, compression, finiteMeans, finiteWeights, finiteCount);
        SerializedTDigestInput finiteInput = new SerializedTDigestInput();
        finiteInput.reset(ByteBuffer.wrap(finiteBytes), false);
        finiteDigest = TDigestUtils.deserializeFinite(finiteInput);
      }
      return new NonFiniteAwareTDigest(finiteDigest, negativeInfinityWeight, positiveInfinityWeight);
    }

    @Override
    public void add(double value) {
      add(value, 1);
    }

    @Override
    public void add(double value, int weight) {
      if (Double.isNaN(value)) {
        throw new IllegalArgumentException("Cannot add NaN to t-digest");
      }
      if (weight <= 0) {
        throw new IllegalArgumentException("TDigest weight must be positive: " + weight);
      }
      checkTotalWeight(getTotalWeight() + weight);
      invalidateCaches();
      if (value == Double.NEGATIVE_INFINITY) {
        _negativeInfinityWeight = _negativeInfinityWeight + weight;
      } else if (value == Double.POSITIVE_INFINITY) {
        _positiveInfinityWeight = _positiveInfinityWeight + weight;
      } else {
        _finiteDigest.add(value, weight);
      }
    }

    @Override
    public void add(TDigest other) {
      checkTotalWeight(getTotalWeight() + other.getTotalWeight());
      if (other instanceof NonFiniteAwareTDigest) {
        NonFiniteAwareTDigest wrapped = (NonFiniteAwareTDigest) other;
        double negativeInfinityWeight = wrapped._negativeInfinityWeight;
        double positiveInfinityWeight = wrapped._positiveInfinityWeight;
        // Merging shared state preserves double-precision weights without narrowing through Centroid.count().
        wrapped.invalidateCaches();
        invalidateCaches();
        if (wrapped._finiteDigest.getTotalWeight() > 0.0) {
          _finiteDigest.add(List.of(wrapped._finiteDigest));
        }
        _negativeInfinityWeight = _negativeInfinityWeight + negativeInfinityWeight;
        _positiveInfinityWeight = _positiveInfinityWeight + positiveInfinityWeight;
        return;
      }

      if (other.getTotalWeight() == 0.0) {
        return;
      }
      invalidateCaches();
      if (Double.isFinite(other.getMin()) && Double.isFinite(other.getMax())) {
        _finiteDigest.add(List.of(other));
        return;
      }
      // Replay the double-precision wire weights: Centroid.count() truncates positive fractional mass to zero.
      ByteBuffer encoded = ByteBuffer.allocate(other.byteSize());
      other.asBytes(encoded);
      encoded.flip();
      byte[] bytes = new byte[encoded.remaining()];
      encoded.get(bytes);
      add(fromBytes(bytes));
    }

    @Override
    public void add(List<? extends TDigest> others) {
      for (TDigest other : others) {
        add(other);
      }
    }

    @Override
    public void compress() {
      getFiniteSerializedBytes();
    }

    @Override
    public long size() {
      return (long) getTotalWeight();
    }

    @Override
    public double getTotalWeight() {
      return _finiteDigest.getTotalWeight() + _negativeInfinityWeight + _positiveInfinityWeight;
    }

    @Override
    public double cdf(double value) {
      if (Double.isNaN(value) || Double.isInfinite(value)) {
        throw new IllegalArgumentException(String.format("Invalid value: %f", value));
      }
      double size = getTotalWeight();
      if (size == 0L) {
        return Double.NaN;
      }
      double finiteSize = _finiteDigest.getTotalWeight();
      double finiteWeight = finiteSize == 0L ? 0.0 : _finiteDigest.cdf(value) * finiteSize;
      return (_negativeInfinityWeight + finiteWeight) / size;
    }

    @Override
    public double quantile(double quantile) {
      if (Double.isNaN(quantile) || quantile < 0.0 || quantile > 1.0) {
        throw new IllegalArgumentException("q should be in [0,1], got " + quantile);
      }
      double size = getTotalWeight();
      if (size == 0L) {
        return Double.NaN;
      }
      double index = quantile * size;
      if (_negativeInfinityWeight > 0L && index < _negativeInfinityWeight) {
        return Double.NEGATIVE_INFINITY;
      }
      if (_positiveInfinityWeight > 0L && index >= size - _positiveInfinityWeight) {
        return Double.POSITIVE_INFINITY;
      }
      double finiteSize = _finiteDigest.getTotalWeight();
      if (finiteSize == 0L) {
        return _negativeInfinityWeight > 0L ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
      }
      double finiteQuantile = (index - _negativeInfinityWeight) / finiteSize;
      return _finiteDigest.quantile(Math.max(0.0, Math.min(finiteQuantile, 1.0)));
    }

    @Override
    public Collection<Centroid> centroids() {
      List<Centroid> centroids = getAllCentroids();
      List<Centroid> copies = new ArrayList<>(centroids.size());
      for (Centroid centroid : centroids) {
        copies.add(new Centroid(centroid.mean(), centroid.count()));
      }
      return copies;
    }

    @Override
    public double compression() {
      return _finiteDigest.compression();
    }

    @Override
    public int byteSize() {
      return getSerializedBytes().length;
    }

    @Override
    public int smallByteSize() {
      return getSerializedBytes().length;
    }

    @Override
    public void asBytes(ByteBuffer buffer) {
      buffer.put(getSerializedBytes());
    }

    @Override
    public void asSmallBytes(ByteBuffer buffer) {
      buffer.put(getSerializedBytes());
    }

    @Override
    public int centroidCount() {
      return getAllCentroids().size();
    }

    @Override
    public double getMin() {
      if (_negativeInfinityWeight > 0L) {
        return Double.NEGATIVE_INFINITY;
      }
      if (_finiteDigest.getTotalWeight() > 0.0) {
        return _finiteDigest.getMin();
      }
      return Double.POSITIVE_INFINITY;
    }

    @Override
    public double getMax() {
      if (_positiveInfinityWeight > 0L) {
        return Double.POSITIVE_INFINITY;
      }
      if (_finiteDigest.getTotalWeight() > 0.0) {
        return _finiteDigest.getMax();
      }
      return Double.NEGATIVE_INFINITY;
    }

    private byte[] serialize(ByteBuffer scratchBuffer) {
      return getSerializedBytes(scratchBuffer).clone();
    }

    private List<Centroid> getFiniteCentroids() {
      if (_finiteCentroids == null) {
        ByteBuffer input = ByteBuffer.wrap(getFiniteSerializedBytes());
        int encoding = input.getInt();
        input.position(input.position() + 2 * Double.BYTES);
        int centroidCount;
        boolean verbose;
        if (encoding == VERBOSE_ENCODING) {
          input.getDouble();
          centroidCount = input.getInt();
          verbose = true;
        } else if (encoding == SMALL_ENCODING) {
          input.getFloat();
          input.getShort();
          input.getShort();
          centroidCount = input.getShort();
          verbose = false;
        } else {
          throw new IllegalStateException("Invalid format for serialized histogram");
        }
        _finiteCentroids = new ArrayList<>(centroidCount);
        for (int i = 0; i < centroidCount; i++) {
          double weight = verbose ? input.getDouble() : input.getFloat();
          double mean = verbose ? input.getDouble() : input.getFloat();
          // The public centroid view retains integer counts; precise aggregation and serialization use bytes.
          appendCentroids(_finiteCentroids, mean, weight, false, false);
        }
      }
      return _finiteCentroids;
    }

    private List<Centroid> getAllCentroids() {
      List<Centroid> finiteCentroids = getFiniteCentroids();
      boolean hasFiniteValues = _finiteDigest.getTotalWeight() > 0.0;
      List<Centroid> centroids = new ArrayList<>(finiteCentroids.size() + 6);
      appendInfinityCentroids(centroids, Double.NEGATIVE_INFINITY, _negativeInfinityWeight, true,
          !hasFiniteValues && _positiveInfinityWeight == 0L);
      centroids.addAll(finiteCentroids);
      appendInfinityCentroids(centroids, Double.POSITIVE_INFINITY, _positiveInfinityWeight,
          !hasFiniteValues && _negativeInfinityWeight == 0L, true);
      return centroids;
    }

    private byte[] getSerializedBytes() {
      return getSerializedBytes(null);
    }

    private byte[] getSerializedBytes(ByteBuffer scratchBuffer) {
      if (_serializedBytes == null) {
        byte[] finiteBytes = getFiniteSerializedBytes(scratchBuffer);
        if (_negativeInfinityWeight == 0L && _positiveInfinityWeight == 0L) {
          _serializedBytes = finiteBytes;
          return _serializedBytes;
        }

        ByteBuffer finite = ByteBuffer.wrap(finiteBytes);
        int encoding = finite.getInt();
        finite.position(finite.position() + 2 * Double.BYTES);
        int finiteCentroidCount;
        boolean finiteVerbose;
        if (encoding == VERBOSE_ENCODING) {
          finite.getDouble();
          finiteCentroidCount = finite.getInt();
          finiteVerbose = true;
        } else if (encoding == SMALL_ENCODING) {
          finite.getFloat();
          finite.getShort();
          finite.getShort();
          finiteCentroidCount = finite.getShort();
          finiteVerbose = false;
        } else {
          throw new IllegalStateException("Invalid format for serialized histogram");
        }

        boolean hasFiniteValues = _finiteDigest.getTotalWeight() > 0.0;
        int negativeInfinityCentroidCount = getInfinityCentroidCount(_negativeInfinityWeight, true,
            !hasFiniteValues && _positiveInfinityWeight == 0L);
        int positiveInfinityCentroidCount = getInfinityCentroidCount(_positiveInfinityWeight,
            !hasFiniteValues && _negativeInfinityWeight == 0L, true);
        int centroidCount = Math.addExact(finiteCentroidCount,
            Math.addExact(negativeInfinityCentroidCount, positiveInfinityCentroidCount));
        ByteBuffer verbose = ByteBuffer.allocate(VERBOSE_HEADER_SIZE + VERBOSE_CENTROID_SIZE * centroidCount);
        verbose.putInt(VERBOSE_ENCODING);
        verbose.putDouble(getMin());
        verbose.putDouble(getMax());
        verbose.putDouble(compression());
        verbose.putInt(centroidCount);
        appendInfinityCentroids(verbose, Double.NEGATIVE_INFINITY, _negativeInfinityWeight, true,
            !hasFiniteValues && _positiveInfinityWeight == 0L);
        for (int i = 0; i < finiteCentroidCount; i++) {
          verbose.putDouble(finiteVerbose ? finite.getDouble() : finite.getFloat());
          verbose.putDouble(finiteVerbose ? finite.getDouble() : finite.getFloat());
        }
        appendInfinityCentroids(verbose, Double.POSITIVE_INFINITY, _positiveInfinityWeight,
            !hasFiniteValues && _negativeInfinityWeight == 0L, true);
        _serializedBytes = TDigestUtils.makeLegacyCompatible(verbose.array());
      }
      return _serializedBytes;
    }

    private byte[] getFiniteSerializedBytes() {
      return getFiniteSerializedBytes(null);
    }

    private byte[] getFiniteSerializedBytes(ByteBuffer scratchBuffer) {
      if (_finiteSerializedBytes == null) {
        // Serialization performs the single final compression pass. Keep these bytes coupled to the derived
        // centroid/final-wire caches so later reads cannot return state from before another destructive compression.
        _finiteSerializedBytes = TDigestUtils.serialize(_finiteDigest, scratchBuffer);
        _finiteCentroids = null;
        _serializedBytes = null;
      }
      return _finiteSerializedBytes;
    }

    private void invalidateCaches() {
      _finiteSerializedBytes = null;
      _finiteCentroids = null;
      _serializedBytes = null;
    }

    private static void appendInfinityCentroids(List<Centroid> centroids, double value, double weight,
        boolean unitWeightAtStart, boolean unitWeightAtEnd) {
      appendCentroids(centroids, value, weight, unitWeightAtStart, unitWeightAtEnd);
    }

    private static void appendCentroids(List<Centroid> centroids, double value, double weight,
        boolean unitWeightAtStart, boolean unitWeightAtEnd) {
      if (weight == 0L) {
        return;
      }
      if (unitWeightAtStart && weight >= (unitWeightAtEnd ? 2.0 : 1.0)) {
        centroids.add(new Centroid(value, 1));
        weight -= 1.0;
      }
      boolean appendUnitWeightAtEnd = unitWeightAtEnd && weight >= 1.0;
      if (appendUnitWeightAtEnd) {
        weight--;
      }
      if (weight > 0.0) {
        // The public int view narrows each double-weight wire centroid, just as the accumulator does. Internal
        // replay consumes its wire bytes, so neither saturated nor zero integer counts lose aggregation mass.
        centroids.add(new Centroid(value, (int) weight));
      }
      if (appendUnitWeightAtEnd) {
        centroids.add(new Centroid(value, 1));
      }
    }

    private static void appendInfinityCentroids(ByteBuffer buffer, double value, double weight,
        boolean unitWeightAtStart, boolean unitWeightAtEnd) {
      if (!(weight > 0.0)) {
        return;
      }
      boolean splitStart = unitWeightAtStart && weight >= (unitWeightAtEnd ? 2.0 : 1.0);
      if (splitStart) {
        buffer.putDouble(1.0);
        buffer.putDouble(value);
        weight -= 1.0;
      }
      boolean splitEnd = unitWeightAtEnd && weight >= 1.0;
      if (splitEnd) {
        weight -= 1.0;
      }
      if (weight > 0.0) {
        buffer.putDouble(weight);
        buffer.putDouble(value);
      }
      if (splitEnd) {
        buffer.putDouble(1.0);
        buffer.putDouble(value);
      }
    }

    private static int getInfinityCentroidCount(double weight, boolean unitWeightAtStart,
        boolean unitWeightAtEnd) {
      if (!(weight > 0.0)) {
        return 0;
      }
      int count = 0;
      if (unitWeightAtStart && weight >= (unitWeightAtEnd ? 2.0 : 1.0)) {
        count++;
        weight -= 1.0;
      }
      if (unitWeightAtEnd && weight >= 1.0) {
        count++;
        weight -= 1.0;
      }
      return count + (weight > 0.0 ? 1 : 0);
    }

    private static void checkTotalWeight(double weight) {
      if (!(weight >= 0.0) || !Double.isFinite(weight)) {
        throw new IllegalArgumentException("Invalid TDigest total weight: " + weight);
      }
    }

    private static byte[] toVerboseBytes(double min, double max, double compression, double[] means,
        double[] weights, int count) {
      ByteBuffer buffer = ByteBuffer.allocate(VERBOSE_HEADER_SIZE + VERBOSE_CENTROID_SIZE * count);
      buffer.putInt(VERBOSE_ENCODING);
      buffer.putDouble(min);
      buffer.putDouble(max);
      buffer.putDouble(compression);
      buffer.putInt(count);
      for (int i = 0; i < count; i++) {
        buffer.putDouble(weights[i]);
        buffer.putDouble(means[i]);
      }
      return buffer.array();
    }
  }
}
