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

import com.tdunning.math.stats.Centroid;
import com.tdunning.math.stats.ScaleFunction;
import com.tdunning.math.stats.TDigest;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
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

  /**
   * Adds a raw value (single value or multi-value array) to the TDigest.
   */
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
    int requiredCapacity = getMaxVerboseByteSize(getDefaultCentroidCapacity(value.compression()));
    if (_serializationBuffer == null || _serializationBuffer.capacity() < requiredCapacity) {
      _serializationBuffer = ByteBuffer.allocate(requiredCapacity);
    }
    byte[] bytes = asNonFiniteAware(value).serialize(_serializationBuffer);
    _maxByteSize = Math.max(_maxByteSize, bytes.length);
    return bytes;
  }

  @Override
  public TDigest deserializeAggregatedValue(byte[] bytes) {
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
    long maxCentroids = Math.min(value.size(), defaultCapacity);
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
    long legacyCapacity = Math.addExact(Math.multiplyExact(2L, (long) Math.ceil(compression)),
        DEFAULT_CENTROID_CAPACITY_PADDING);
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
    private long _negativeInfinityWeight;
    private long _positiveInfinityWeight;
    private byte[] _finiteSerializedBytes;
    private List<Centroid> _finiteCentroids;
    private byte[] _serializedBytes;

    private NonFiniteAwareTDigest(double compression) {
      this(TDigestUtils.createMergingDigestWithLegacyBuffer(compression), 0L, 0L);
    }

    private NonFiniteAwareTDigest(TDigest finiteDigest, long negativeInfinityWeight, long positiveInfinityWeight) {
      _finiteDigest = finiteDigest;
      _negativeInfinityWeight = negativeInfinityWeight;
      _positiveInfinityWeight = positiveInfinityWeight;
    }

    private NonFiniteAwareTDigest copy() {
      invalidateCaches();
      int minimumMainCapacity = Math.addExact(_finiteDigest.centroidCount(), 2);
      TDigest finiteDigest = TDigestUtils.createMergingDigestWithLegacyBuffer(compression(), minimumMainCapacity);
      if (_finiteDigest.size() > 0L) {
        finiteDigest.add(List.of(_finiteDigest));
      }
      return new NonFiniteAwareTDigest(finiteDigest, _negativeInfinityWeight, _positiveInfinityWeight);
    }

    private static NonFiniteAwareTDigest fromBytes(byte[] bytes) {
      TDigestUtils.validateSerialized(bytes);
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
      long negativeInfinityWeight = 0L;
      long positiveInfinityWeight = 0L;
      for (int i = 0; i < centroidCount; i++) {
        double weight = verbose ? input.getDouble() : input.getFloat();
        double mean = verbose ? input.getDouble() : input.getFloat();
        if (mean == Double.NEGATIVE_INFINITY) {
          negativeInfinityWeight = Math.addExact(negativeInfinityWeight, toLongWeight(weight));
        } else if (mean == Double.POSITIVE_INFINITY) {
          positiveInfinityWeight = Math.addExact(positiveInfinityWeight, toLongWeight(weight));
        } else if (Double.isNaN(mean)) {
          throw new IllegalArgumentException("Cannot deserialize a TDigest with a NaN centroid mean");
        }
      }
      if (negativeInfinityWeight == 0L && positiveInfinityWeight == 0L) {
        return new NonFiniteAwareTDigest(TDigestUtils.deserializeFiniteWithLegacyBuffer(bytes), 0L, 0L);
      }

      double[] finiteMeans = new double[centroidCount];
      double[] finiteWeights = new double[centroidCount];
      int finiteCount = 0;
      input.position(centroidOffset);
      for (int i = 0; i < centroidCount; i++) {
        double weight = verbose ? input.getDouble() : input.getFloat();
        double mean = verbose ? input.getDouble() : input.getFloat();
        if (Double.isFinite(mean)) {
          finiteWeights[finiteCount] = weight;
          finiteMeans[finiteCount] = mean;
          finiteCount++;
        }
      }

      TDigest finiteDigest;
      if (finiteCount == 0) {
        finiteDigest = TDigestUtils.createMergingDigestWithLegacyBuffer(compression);
      } else {
        double finiteMin = Double.isFinite(encodedMin) ? encodedMin : finiteMeans[0];
        double finiteMax = Double.isFinite(encodedMax) ? encodedMax : finiteMeans[finiteCount - 1];
        byte[] finiteBytes = toVerboseBytes(finiteMin, finiteMax, compression, finiteMeans, finiteWeights, finiteCount);
        finiteDigest = TDigestUtils.deserializeFiniteWithLegacyBuffer(
            TDigestUtils.makeLegacyCompatible(finiteBytes));
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
      invalidateCaches();
      if (value == Double.NEGATIVE_INFINITY) {
        _negativeInfinityWeight = Math.addExact(_negativeInfinityWeight, weight);
      } else if (value == Double.POSITIVE_INFINITY) {
        _positiveInfinityWeight = Math.addExact(_positiveInfinityWeight, weight);
      } else {
        _finiteDigest.add(value, weight);
      }
    }

    @Override
    public void add(TDigest other) {
      if (other instanceof NonFiniteAwareTDigest) {
        NonFiniteAwareTDigest wrapped = (NonFiniteAwareTDigest) other;
        long negativeInfinityWeight = wrapped._negativeInfinityWeight;
        long positiveInfinityWeight = wrapped._positiveInfinityWeight;
        // MergingDigest.add(List) copies the implementation's double-precision weight arrays directly. The generic
        // add(TDigest) path goes through Centroid.count(), which is limited to an int and truncates large weights.
        wrapped.invalidateCaches();
        invalidateCaches();
        if (wrapped._finiteDigest.size() > 0L) {
          _finiteDigest.add(List.of(wrapped._finiteDigest));
        }
        _negativeInfinityWeight = Math.addExact(_negativeInfinityWeight, negativeInfinityWeight);
        _positiveInfinityWeight = Math.addExact(_positiveInfinityWeight, positiveInfinityWeight);
        return;
      }

      if (other.size() == 0L) {
        return;
      }
      invalidateCaches();
      if (Double.isFinite(other.getMin()) && Double.isFinite(other.getMax())) {
        _finiteDigest.add(List.of(other));
        return;
      }
      for (Centroid centroid : other.centroids()) {
        add(centroid.mean(), centroid.count());
      }
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
      return Math.addExact(Math.addExact(_finiteDigest.size(), _negativeInfinityWeight), _positiveInfinityWeight);
    }

    @Override
    public double cdf(double value) {
      if (Double.isNaN(value) || Double.isInfinite(value)) {
        throw new IllegalArgumentException(String.format("Invalid value: %f", value));
      }
      long size = size();
      if (size == 0L) {
        return Double.NaN;
      }
      long finiteSize = _finiteDigest.size();
      double finiteWeight = finiteSize == 0L ? 0.0 : _finiteDigest.cdf(value) * finiteSize;
      return (_negativeInfinityWeight + finiteWeight) / size;
    }

    @Override
    public double quantile(double quantile) {
      if (Double.isNaN(quantile) || quantile < 0.0 || quantile > 1.0) {
        throw new IllegalArgumentException("q should be in [0,1], got " + quantile);
      }
      long size = size();
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
      long finiteSize = _finiteDigest.size();
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
        copies.add(Centroid.createWeighted(centroid.mean(), centroid.count(), centroid.data()));
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
    public TDigest recordAllData() {
      invalidateCaches();
      _finiteDigest.recordAllData();
      return this;
    }

    @Override
    public boolean isRecording() {
      return _finiteDigest.isRecording();
    }

    @Override
    public void setScaleFunction(ScaleFunction scaleFunction) {
      invalidateCaches();
      _finiteDigest.setScaleFunction(scaleFunction);
      super.setScaleFunction(scaleFunction);
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
      if (_finiteDigest.size() > 0L) {
        return _finiteDigest.getMin();
      }
      return Double.POSITIVE_INFINITY;
    }

    @Override
    public double getMax() {
      if (_positiveInfinityWeight > 0L) {
        return Double.POSITIVE_INFINITY;
      }
      if (_finiteDigest.size() > 0L) {
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
          appendCentroids(_finiteCentroids, mean, toLongWeight(weight), false, false);
        }
      }
      return _finiteCentroids;
    }

    private List<Centroid> getAllCentroids() {
      List<Centroid> finiteCentroids = getFiniteCentroids();
      boolean hasFiniteValues = _finiteDigest.size() > 0L;
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

        boolean hasFiniteValues = _finiteDigest.size() > 0L;
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

    private static void appendInfinityCentroids(List<Centroid> centroids, double value, long weight,
        boolean unitWeightAtStart, boolean unitWeightAtEnd) {
      appendCentroids(centroids, value, weight, unitWeightAtStart, unitWeightAtEnd);
    }

    private static void appendCentroids(List<Centroid> centroids, double value, long weight,
        boolean unitWeightAtStart, boolean unitWeightAtEnd) {
      if (weight == 0L) {
        return;
      }
      if (unitWeightAtStart) {
        centroids.add(Centroid.createWeighted(value, 1, null));
        weight--;
      }
      boolean appendUnitWeightAtEnd = unitWeightAtEnd && weight > 0L;
      if (appendUnitWeightAtEnd) {
        weight--;
      }
      while (weight > 0L) {
        int centroidWeight = (int) Math.min(weight, Integer.MAX_VALUE);
        centroids.add(Centroid.createWeighted(value, centroidWeight, null));
        weight -= centroidWeight;
      }
      if (appendUnitWeightAtEnd) {
        centroids.add(Centroid.createWeighted(value, 1, null));
      }
    }

    private static void appendInfinityCentroids(ByteBuffer buffer, double value, long weight,
        boolean unitWeightAtStart, boolean unitWeightAtEnd) {
      if (weight == 0L) {
        return;
      }
      if (unitWeightAtStart) {
        buffer.putDouble(1.0);
        buffer.putDouble(value);
        weight--;
      }
      boolean appendUnitWeightAtEnd = unitWeightAtEnd && weight > 0L;
      if (appendUnitWeightAtEnd) {
        weight--;
      }
      while (weight > 0L) {
        int centroidWeight = (int) Math.min(weight, Integer.MAX_VALUE);
        buffer.putDouble(centroidWeight);
        buffer.putDouble(value);
        weight -= centroidWeight;
      }
      if (appendUnitWeightAtEnd) {
        buffer.putDouble(1.0);
        buffer.putDouble(value);
      }
    }

    private static int getInfinityCentroidCount(long weight, boolean unitWeightAtStart, boolean unitWeightAtEnd) {
      if (weight == 0L) {
        return 0;
      }
      int count = 0;
      if (unitWeightAtStart) {
        count++;
        weight--;
      }
      if (unitWeightAtEnd && weight > 0L) {
        count++;
        weight--;
      }
      long fullCentroids = weight / Integer.MAX_VALUE;
      long partialCentroid = weight % Integer.MAX_VALUE == 0L ? 0L : 1L;
      return Math.addExact(count, Math.toIntExact(fullCentroids + partialCentroid));
    }

    private static long toLongWeight(double weight) {
      if (!(weight > 0.0) || weight >= 0x1p63 || weight != Math.rint(weight)) {
        throw new IllegalArgumentException("Invalid TDigest centroid weight: " + weight);
      }
      return (long) weight;
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
