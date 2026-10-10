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
package org.apache.pinot.segment.local.customobject.tdigest;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.customobject.tdigest.TDigestCodec.SerializedTDigestMetadata;


/// Stores non-finite values as exact tail masses while delegating finite values to an owned native digest.
/// Emits legacy t-digest bytes and preserves historical fractional endpoint provenance across merges and copies.
/// Instances are mutable and require external synchronization.
public final class NonFiniteAwareTDigest extends TDigest {
  private final PercentileTDigestAccumulator _finiteDigest;
  private double _negativeInfinityWeight;
  private double _positiveInfinityWeight;
  private List<Centroid> _finiteCentroids;
  private byte[] _originalFractionalBytes;

  public static NonFiniteAwareTDigest forLegacyAggregation(double compression) {
    return new NonFiniteAwareTDigest(PercentileTDigestAccumulator.forLegacyAggregation(compression), 0.0, 0.0);
  }

  private NonFiniteAwareTDigest(PercentileTDigestAccumulator finiteDigest, double negativeInfinityWeight,
      double positiveInfinityWeight) {
    _finiteDigest = finiteDigest;
    _negativeInfinityWeight = negativeInfinityWeight;
    _positiveInfinityWeight = positiveInfinityWeight;
  }

  public NonFiniteAwareTDigest copy() {
    if (_originalFractionalBytes != null) {
      return fromBytes(_originalFractionalBytes);
    }
    PercentileTDigestAccumulator source = _finiteDigest;
    PercentileTDigestAccumulator finiteDigest;
    if (!source.hasValidStatistics() || source.hasOriginalFractionalPayload()) {
      finiteDigest = PercentileTDigestAccumulator.fromBytes(source.serialize());
    } else {
      finiteDigest = PercentileTDigestAccumulator.forLegacyAggregation(compression());
      if (!source.isEmpty()) {
        finiteDigest.add(source);
        invalidateDerivedCaches();
      }
    }
    if (source.hasValidStatistics()) {
      finiteDigest.inheritHistoricalFractionalBoundaries(source);
    }
    return new NonFiniteAwareTDigest(finiteDigest, _negativeInfinityWeight, _positiveInfinityWeight);
  }

  public static NonFiniteAwareTDigest fromBytes(byte[] bytes) {
    SerializedTDigestInput validated = new SerializedTDigestInput();
    validated.reset(bytes);
    SerializedTDigestMetadata metadata = validated.getMetadata();
    if (!metadata.hasNonFiniteMeans() || metadata.needsLegacyFallback()) {
      return new NonFiniteAwareTDigest(PercentileTDigestAccumulator.fromBytes(validated), 0.0, 0.0);
    }
    double encodedMin = metadata.min();
    double encodedMax = metadata.max();
    double compression = metadata.compression();
    int centroidCount = metadata.centroidCount();
    double negativeInfinityWeight = 0.0;
    double positiveInfinityWeight = 0.0;
    double[] finiteMeans = new double[centroidCount];
    double[] finiteWeights = new double[centroidCount];
    TDigestCodec.inspectSerialized(ByteBuffer.wrap(bytes), metadata, finiteMeans, finiteWeights);
    int finiteCount = 0;
    double finiteMin = Double.POSITIVE_INFINITY;
    double finiteMax = Double.NEGATIVE_INFINITY;
    for (int i = 0; i < centroidCount; i++) {
      double weight = finiteWeights[i];
      double mean = finiteMeans[i];
      if (weight == 0.0) {
        continue;
      }
      if (mean == Double.NEGATIVE_INFINITY) {
        negativeInfinityWeight += weight;
      } else if (mean == Double.POSITIVE_INFINITY) {
        positiveInfinityWeight += weight;
      } else {
        finiteWeights[finiteCount] = weight;
        finiteMeans[finiteCount] = mean;
        finiteMin = Math.min(finiteMin, mean);
        finiteMax = Math.max(finiteMax, mean);
        finiteCount++;
      }
    }

    PercentileTDigestAccumulator finiteDigest =
        PercentileTDigestAccumulator.forLegacyAggregation(compression);
    if (finiteCount > 0) {
      finiteMin = Double.isFinite(encodedMin) ? encodedMin : finiteMin;
      finiteMax = Double.isFinite(encodedMax) ? encodedMax : finiteMax;
      finiteDigest.addCentroids(finiteMeans, finiteWeights, finiteCount, finiteMin, finiteMax, true);
    }
    validated.recordHistoricalFractionalBoundaries(finiteDigest);
    NonFiniteAwareTDigest wrapped =
        new NonFiniteAwareTDigest(finiteDigest, negativeInfinityWeight, positiveInfinityWeight);
    if (metadata.fractionalWeights()) {
      wrapped._originalFractionalBytes = bytes.clone();
    }
    return wrapped;
  }

  @Override
  public void add(double value) {
    add(value, 1);
  }

  @Override
  public void add(double value, double weight) {
    if (Double.isNaN(value)) {
      throw new IllegalArgumentException("Cannot add NaN to t-digest");
    }
    if (!(weight > 0.0) || !Double.isFinite(weight)) {
      throw new IllegalArgumentException("TDigest weight must be positive: " + weight);
    }
    requireMutable();
    checkTotalWeight(getTotalWeight() + weight);
    if (value == Double.NEGATIVE_INFINITY) {
      _negativeInfinityWeight += weight;
    } else if (value == Double.POSITIVE_INFINITY) {
      _positiveInfinityWeight += weight;
    } else {
      _finiteDigest.add(value, weight);
    }
    mutated();
  }

  @Override
  public void add(TDigest other) {
    if (other.isEmpty()) {
      return;
    }
    requireMutable();
    if (!other.hasValidStatistics() && !isEmpty()) {
      throw corruptedMutation();
    }
    if (other.hasValidStatistics()) {
      checkTotalWeight(getTotalWeight() + other.getTotalWeight());
    }
    if (other instanceof NonFiniteAwareTDigest) {
      NonFiniteAwareTDigest wrapped = (NonFiniteAwareTDigest) other;
      double negativeInfinityWeight = wrapped._negativeInfinityWeight;
      double positiveInfinityWeight = wrapped._positiveInfinityWeight;
      if (!wrapped._finiteDigest.isEmpty()) {
        _finiteDigest.add(wrapped._finiteDigest);
        wrapped.invalidateDerivedCaches();
      }
      if (wrapped.hasValidStatistics()) {
        _finiteDigest.inheritHistoricalFractionalBoundaries(wrapped);
      }
      _negativeInfinityWeight += negativeInfinityWeight;
      _positiveInfinityWeight += positiveInfinityWeight;
      mutated();
      return;
    }
    if (!other.hasValidStatistics() || Double.isFinite(other.getMin()) && Double.isFinite(other.getMax())) {
      _finiteDigest.add(other);
      mutated();
      return;
    }
    // Read precise centroid weights directly; generic sources need no full digest serialization for their tails.
    Collection<Centroid> centroids = other.centroids();
    double[] finiteMeans = new double[centroids.size()];
    double[] finiteWeights = new double[centroids.size()];
    int finiteCount = 0;
    double finiteMin = Double.POSITIVE_INFINITY;
    double finiteMax = Double.NEGATIVE_INFINITY;
    double negativeInfinityWeight = 0.0;
    double positiveInfinityWeight = 0.0;
    double totalWeight = getTotalWeight();
    for (Centroid centroid : centroids) {
      double mean = centroid.mean();
      double weight = centroid.weight();
      if (weight == 0.0) {
        continue;
      }
      if (!(weight > 0.0) || !Double.isFinite(weight) || Double.isNaN(mean)) {
        throw new IllegalArgumentException("Invalid TDigest centroid");
      }
      totalWeight += weight;
      if (mean == Double.NEGATIVE_INFINITY) {
        negativeInfinityWeight += weight;
      } else if (mean == Double.POSITIVE_INFINITY) {
        positiveInfinityWeight += weight;
      } else {
        finiteMeans[finiteCount] = mean;
        finiteWeights[finiteCount++] = weight;
        finiteMin = Math.min(finiteMin, mean);
        finiteMax = Math.max(finiteMax, mean);
      }
    }
    checkTotalWeight(totalWeight);
    if (finiteCount > 0) {
      finiteMin = Double.isFinite(other.getMin()) ? other.getMin() : finiteMin;
      finiteMax = Double.isFinite(other.getMax()) ? other.getMax() : finiteMax;
      _finiteDigest.addCentroids(
          finiteMeans, finiteWeights, finiteCount, finiteMin, finiteMax, false);
    }
    _finiteDigest.inheritHistoricalFractionalBoundaries(other);
    _negativeInfinityWeight += negativeInfinityWeight;
    _positiveInfinityWeight += positiveInfinityWeight;
    mutated();
  }

  private void requireMutable() {
    if (!hasValidStatistics()) {
      throw corruptedMutation();
    }
  }

  private static IllegalArgumentException corruptedMutation() {
    return new IllegalArgumentException("Cannot merge or mutate a historically corrupted TDigest; "
        + "rebuild stored digests from source data before merging");
  }

  private void mutated() {
    _originalFractionalBytes = null;
    invalidateDerivedCaches();
  }

  @Override
  public void compress() {
    _finiteDigest.compress();
    invalidateDerivedCaches();
  }

  @Override
  public double getTotalWeight() {
    return _finiteDigest.getTotalWeight() + _negativeInfinityWeight + _positiveInfinityWeight;
  }

  @Override
  double getHistoricalFractionalBoundaryMean(boolean lowerBoundary) {
    return _finiteDigest.getHistoricalFractionalBoundaryMean(lowerBoundary);
  }

  @Override
  public boolean hasValidStatistics() {
    return _finiteDigest.hasValidStatistics();
  }

  @Override
  public double cdf(double value) {
    if (Double.isNaN(value) || Double.isInfinite(value)) {
      throw new IllegalArgumentException(String.format("Invalid value: %f", value));
    }
    double size = getTotalWeight();
    if (size == 0.0 || !hasValidStatistics()) {
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
    if (size == 0.0 || !hasValidStatistics()) {
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
    return getAllCentroids();
  }

  @Override
  public double compression() {
    return _finiteDigest.compression();
  }

  @Override
  public int maxSerializedByteSize() {
    if (_originalFractionalBytes != null) {
      return _originalFractionalBytes.length;
    }
    if (_negativeInfinityWeight == 0.0 && _positiveInfinityWeight == 0.0) {
      return _finiteDigest.maxSerializedByteSize();
    }
    int finiteByteSize = _finiteDigest.maxSerializedByteSize();
    PercentileTDigestAccumulator finiteDigest = _finiteDigest;
    if (finiteDigest.hasOriginalFractionalPayload()) {
      // Infinite tails can make retained fractional boundaries interior; their finite view may split endpoints.
      finiteByteSize = Math.max(finiteByteSize,
          TDigestCodec.getMaxLegacyCompatibleByteSize(compression(), finiteDigest.getCentroidCountUpperBound()));
    }
    boolean hasFiniteValues = _finiteDigest.getTotalWeight() > 0.0;
    int negativeInfinityCount = getInfinityCentroidCount(_negativeInfinityWeight, true,
        !hasFiniteValues && _positiveInfinityWeight == 0.0);
    int positiveInfinityCount = getInfinityCentroidCount(_positiveInfinityWeight,
        !hasFiniteValues && _negativeInfinityWeight == 0.0, true);
    int tailCount = Math.addExact(negativeInfinityCount, positiveInfinityCount);
    int tailByteSize = Math.multiplyExact(TDigestCodec.VERBOSE_CENTROID_SIZE, tailCount);
    long finiteCount = hasFiniteValues ? finiteDigest.getCentroidCountUpperBound() : 0L;
    int compatibleBound =
        TDigestCodec.getMaxLegacyCompatibleByteSize(compression(), Math.addExact(finiteCount, tailCount));
    return Math.min(Math.addExact(finiteByteSize, tailByteSize), compatibleBound);
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
    if (!_finiteDigest.isEmpty()) {
      return _finiteDigest.getMin();
    }
    return Double.POSITIVE_INFINITY;
  }

  @Override
  public double getMax() {
    if (_positiveInfinityWeight > 0L) {
      return Double.POSITIVE_INFINITY;
    }
    if (!_finiteDigest.isEmpty()) {
      return _finiteDigest.getMax();
    }
    return Double.NEGATIVE_INFINITY;
  }

  private List<Centroid> getFiniteCentroids() {
    if (_finiteCentroids == null) {
      // Match the wire view without serializing finite-only fractional boundaries surrounded by infinite tails.
      PercentileTDigestAccumulator finiteDigest = _finiteDigest;
      finiteDigest.prepareCentroidsForSerialization();
      invalidateDerivedCaches();
      _finiteCentroids = new ArrayList<>(_finiteDigest.centroids());
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
    if (centroids.size() > 1
        && (centroids.getFirst().weight() > 1.0 || centroids.getLast().weight() > 1.0)) {
      int count = centroids.size();
      double[] means = new double[Math.addExact(count, 2)];
      double[] weights = new double[means.length];
      for (int i = 0; i < count; i++) {
        Centroid centroid = centroids.get(i);
        means[i] = centroid.mean();
        weights[i] = centroid.weight();
      }
      count = PercentileTDigestAccumulator.normalizeSerializedBoundaries(
          means, weights, count, getMin(), getMax());
      centroids.clear();
      for (int i = 0; i < count; i++) {
        centroids.add(new Centroid(means[i], weights[i]));
      }
    }
    return centroids;
  }

  @Override
  public byte[] serialize() {
    if (_originalFractionalBytes != null) {
      return _originalFractionalBytes.clone();
    }
    if (_negativeInfinityWeight == 0.0 && _positiveInfinityWeight == 0.0) {
      byte[] bytes = _finiteDigest.serialize();
      invalidateDerivedCaches();
      return bytes;
    }
    _finiteDigest.prepareCentroidsForSerialization();
    invalidateDerivedCaches();
    int finiteCentroidCount = _finiteDigest.centroidCount();
    boolean hasFiniteValues = _finiteDigest.getTotalWeight() > 0.0;
    int negativeInfinityCentroidCount = getInfinityCentroidCount(_negativeInfinityWeight, true,
        !hasFiniteValues && _positiveInfinityWeight == 0.0);
    int positiveInfinityCentroidCount = getInfinityCentroidCount(_positiveInfinityWeight,
        !hasFiniteValues && _negativeInfinityWeight == 0.0, true);
    int centroidCount = Math.addExact(finiteCentroidCount,
        Math.addExact(negativeInfinityCentroidCount, positiveInfinityCentroidCount));
    double[] means = new double[Math.addExact(centroidCount, 2)];
    double[] weights = new double[means.length];
    _finiteDigest.copyCentroids(means, weights, negativeInfinityCentroidCount);
    appendInfinityCentroids(means, weights, 0, Double.NEGATIVE_INFINITY, _negativeInfinityWeight, true,
        !hasFiniteValues && _positiveInfinityWeight == 0.0);
    appendInfinityCentroids(means, weights, negativeInfinityCentroidCount + finiteCentroidCount,
        Double.POSITIVE_INFINITY, _positiveInfinityWeight,
        !hasFiniteValues && _negativeInfinityWeight == 0.0, true);
    centroidCount = PercentileTDigestAccumulator.normalizeSerializedBoundaries(
        means, weights, centroidCount, getMin(), getMax());
    return TDigestCodec.serializeCompatibleCentroids(compression(), getMin(), getMax(), means, weights, centroidCount,
        _finiteDigest.hasInheritedFractionalBoundaryEncoding(means, weights, centroidCount));
  }

  private void invalidateDerivedCaches() {
    _finiteCentroids = null;
  }

  private static void appendInfinityCentroids(List<Centroid> centroids, double value, double weight,
      boolean unitWeightAtStart, boolean unitWeightAtEnd) {
    appendInfinityCentroids(centroids, null, null, 0, value, weight, unitWeightAtStart, unitWeightAtEnd);
  }

  // A null destination counts the same split without allocating centroids or temporary arrays.
  private static int appendInfinityCentroids(@Nullable List<Centroid> centroids, @Nullable double[] means,
      @Nullable double[] weights, int offset, double value, double weight, boolean unitWeightAtStart,
      boolean unitWeightAtEnd) {
    if (!(weight > 0.0)) {
      return 0;
    }
    int count = 0;
    if (unitWeightAtStart && weight >= (unitWeightAtEnd ? 2.0 : 1.0)) {
      appendInfinityCentroid(centroids, means, weights, offset + count++, value, 1.0);
      weight -= 1.0;
    }
    boolean appendUnitWeightAtEnd = unitWeightAtEnd && weight >= 1.0;
    if (appendUnitWeightAtEnd) {
      weight -= 1.0;
    }
    if (weight > 0.0) {
      appendInfinityCentroid(centroids, means, weights, offset + count++, value, weight);
    }
    if (appendUnitWeightAtEnd) {
      appendInfinityCentroid(centroids, means, weights, offset + count++, value, 1.0);
    }
    return count;
  }

  private static void appendInfinityCentroid(@Nullable List<Centroid> centroids, @Nullable double[] means,
      @Nullable double[] weights, int offset, double value, double weight) {
    if (centroids != null) {
      centroids.add(new Centroid(value, weight));
    } else if (means != null && weights != null) {
      means[offset] = value;
      weights[offset] = weight;
    }
  }

  private static void appendInfinityCentroids(double[] means, double[] weights, int offset, double value,
      double weight, boolean unitWeightAtStart, boolean unitWeightAtEnd) {
    appendInfinityCentroids(null, means, weights, offset, value, weight, unitWeightAtStart, unitWeightAtEnd);
  }

  private static int getInfinityCentroidCount(double weight, boolean unitWeightAtStart,
      boolean unitWeightAtEnd) {
    return appendInfinityCentroids(null, null, null, 0, 0.0, weight, unitWeightAtStart, unitWeightAtEnd);
  }

  private static void checkTotalWeight(double weight) {
    if (!(weight >= 0.0) || !Double.isFinite(weight)) {
      throw new IllegalArgumentException("Invalid TDigest total weight: " + weight);
    }
  }
}
