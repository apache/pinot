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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.customobject.tdigest.TDigestCodec.SerializedTDigestMetadata;


/// Stores non-finite values as tail masses while delegating finite values to an owned native digest.
/// Emits legacy t-digest bytes and preserves historical fractional endpoint provenance across merges and copies.
/// Instances are mutable and require external synchronization.
public final class NonFiniteAwareTDigest extends TDigest {
  private TDigestAccumulator _finiteDigest;
  private double _negativeInfinityWeight;
  private double _positiveInfinityWeight;
  private List<Centroid> _finiteCentroids;
  private byte[] _originalBytes;
  private boolean _unrepresentableInfinityTail;
  private boolean _pendingHistoricalBoundaryRepair;
  private double _historicalMin;
  private double _historicalMax;

  public static NonFiniteAwareTDigest forLegacyAggregation(double compression) {
    return new NonFiniteAwareTDigest(TDigestAccumulator.forLegacyAggregation(compression), 0.0, 0.0);
  }

  private NonFiniteAwareTDigest(TDigestAccumulator finiteDigest, double negativeInfinityWeight,
      double positiveInfinityWeight) {
    _finiteDigest = finiteDigest;
    _negativeInfinityWeight = negativeInfinityWeight;
    _positiveInfinityWeight = positiveInfinityWeight;
  }

  /// Snapshots the aggregation without flushing or compressing the source's buffered inputs.
  public NonFiniteAwareTDigest copy() {
    NonFiniteAwareTDigest copy =
        new NonFiniteAwareTDigest(_finiteDigest.copy(), _negativeInfinityWeight, _positiveInfinityWeight);
    copy._originalBytes = _originalBytes == null ? null : _originalBytes.clone();
    copy._unrepresentableInfinityTail = _unrepresentableInfinityTail;
    copy._pendingHistoricalBoundaryRepair = _pendingHistoricalBoundaryRepair;
    copy._historicalMin = _historicalMin;
    copy._historicalMax = _historicalMax;
    return copy;
  }

  /// Copies a digest, snapshotting native accumulator buffers before any consuming conversion.
  public static NonFiniteAwareTDigest copyOf(TDigest digest) {
    if (digest instanceof NonFiniteAwareTDigest) {
      return ((NonFiniteAwareTDigest) digest).copy();
    }
    NonFiniteAwareTDigest copy = forLegacyAggregation(digest.compression());
    copy.add(digest instanceof TDigestAccumulator ? ((TDigestAccumulator) digest).copy() : digest);
    return copy;
  }

  public static NonFiniteAwareTDigest fromBytes(byte[] bytes) {
    SerializedTDigestInput validated = new SerializedTDigestInput();
    validated.reset(bytes);
    SerializedTDigestMetadata metadata = validated.getMetadata();
    if (!metadata.hasNonFiniteMeans() || metadata.needsLegacyFallback()) {
      return new NonFiniteAwareTDigest(TDigestAccumulator.fromBytes(validated), 0.0, 0.0);
    }
    validated.decode();
    if (!TDigestAccumulator.canNormalizeBoundaries(validated._means, validated._weights,
        validated._numCentroids, metadata.min(), metadata.max())) {
      return new NonFiniteAwareTDigest(TDigestAccumulator.fromBytes(validated), 0.0, 0.0);
    }
    double encodedMin = metadata.min();
    double encodedMax = metadata.max();
    double compression = metadata.compression();
    int centroidCount = validated._numCentroids;
    double negativeInfinityWeight = 0.0;
    double positiveInfinityWeight = 0.0;
    double[] finiteMeans = validated._means;
    double[] finiteWeights = validated._weights;
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

    TDigestAccumulator finiteDigest =
        TDigestAccumulator.forLegacyAggregation(compression);
    if (negativeInfinityWeight > 0.0 || positiveInfinityWeight > 0.0) {
      finiteDigest.deferBoundaryNormalization();
    }
    if (finiteCount > 0) {
      finiteMin = Double.isFinite(encodedMin) ? encodedMin : finiteMin;
      finiteMax = Double.isFinite(encodedMax) ? encodedMax : finiteMax;
      finiteDigest.addHistoricalCentroids(finiteMeans, finiteWeights, finiteCount, finiteMin, finiteMax, true);
    }
    validated.recordHistoricalFractionalBoundaries(finiteDigest);
    NonFiniteAwareTDigest wrapped =
        new NonFiniteAwareTDigest(finiteDigest, negativeInfinityWeight, positiveInfinityWeight);
    wrapped._pendingHistoricalBoundaryRepair = metadata.weightedBoundaries();
    wrapped._historicalMin = encodedMin;
    wrapped._historicalMax = encodedMax;
    wrapped._unrepresentableInfinityTail = !canSplitInfinityWeight(negativeInfinityWeight)
        || !canSplitInfinityWeight(positiveInfinityWeight);
    if (metadata.fractionalWeights() || wrapped._unrepresentableInfinityTail) {
      wrapped._originalBytes = bytes.clone();
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
    TDigestAccumulator.checkTotalWeight(getTotalWeight() + weight);
    double infinityWeight = 0.0;
    if (value == Double.NEGATIVE_INFINITY) {
      addInfinityWeight(_negativeInfinityWeight, weight);
      infinityWeight = addInfinityWeight(boundaryPreparedView()._negativeInfinityWeight, weight);
    } else if (value == Double.POSITIVE_INFINITY) {
      addInfinityWeight(_positiveInfinityWeight, weight);
      infinityWeight = addInfinityWeight(boundaryPreparedView()._positiveInfinityWeight, weight);
    }
    checkSplittableInfinityWeight(infinityWeight);
    requireMutable();
    if (value == Double.NEGATIVE_INFINITY) {
      _finiteDigest.deferBoundaryNormalization();
      _finiteDigest.recordFreshFractionalMean(value, weight);
      _negativeInfinityWeight = infinityWeight;
    } else if (value == Double.POSITIVE_INFINITY) {
      _finiteDigest.deferBoundaryNormalization();
      _finiteDigest.recordFreshFractionalMean(value, weight);
      _positiveInfinityWeight = infinityWeight;
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
    if (!hasValidStatistics()) {
      throw TDigestAccumulator.corruptedMutation();
    }
    if (!other.hasValidStatistics() && !isEmpty()) {
      throw TDigestAccumulator.corruptedMutation();
    }
    if (other.hasValidStatistics()) {
      TDigestAccumulator.checkTotalWeight(getTotalWeight() + other.getTotalWeight());
    }
    if (other instanceof NonFiniteAwareTDigest) {
      NonFiniteAwareTDigest wrapped = (NonFiniteAwareTDigest) other;
      if (isEmpty() && wrapped._unrepresentableInfinityTail) {
        _finiteDigest = wrapped._finiteDigest.copy();
        _negativeInfinityWeight = wrapped._negativeInfinityWeight;
        _positiveInfinityWeight = wrapped._positiveInfinityWeight;
        _originalBytes = wrapped._originalBytes.clone();
        _unrepresentableInfinityTail = true;
        _pendingHistoricalBoundaryRepair = wrapped._pendingHistoricalBoundaryRepair;
        _historicalMin = wrapped._historicalMin;
        _historicalMax = wrapped._historicalMax;
        invalidateDerivedCaches();
        return;
      }
      addInfinityWeight(_negativeInfinityWeight, wrapped._negativeInfinityWeight);
      addInfinityWeight(_positiveInfinityWeight, wrapped._positiveInfinityWeight);
      NonFiniteAwareTDigest preparedReceiver = boundaryPreparedView();
      NonFiniteAwareTDigest preparedSource = wrapped.boundaryPreparedView();
      double negativeInfinityWeight =
          addInfinityWeight(preparedReceiver._negativeInfinityWeight, preparedSource._negativeInfinityWeight);
      double positiveInfinityWeight =
          addInfinityWeight(preparedReceiver._positiveInfinityWeight, preparedSource._positiveInfinityWeight);
      checkSplittableInfinityWeight(negativeInfinityWeight);
      checkSplittableInfinityWeight(positiveInfinityWeight);
      requireMutable();
      wrapped.prepareHistoricalBoundaries();
      if (negativeInfinityWeight > 0.0 || positiveInfinityWeight > 0.0) {
        _finiteDigest.deferBoundaryNormalization();
      }
      try {
        if (!wrapped._finiteDigest.isEmpty()) {
          _finiteDigest.add(wrapped._finiteDigest);
        }
      } catch (RuntimeException | Error e) {
        restoreBoundaryNormalizationAfterFailedMerge();
        throw e;
      } finally {
        // Native merging can compress the source before a later validation failure.
        wrapped.invalidateDerivedCaches();
      }
      if (wrapped.hasValidStatistics()) {
        _finiteDigest.inheritHistoricalFractionalBoundaries(wrapped);
      }
      _negativeInfinityWeight = negativeInfinityWeight;
      _positiveInfinityWeight = positiveInfinityWeight;
      mutated();
      return;
    }
    if (!other.hasValidStatistics() || Double.isFinite(other.getMin()) && Double.isFinite(other.getMax())) {
      requireMutable();
      _finiteDigest.add(other);
      mutated();
      return;
    }
    // Read precise centroid weights directly; generic sources need no full digest serialization for their tails.
    if (other instanceof TDigestAccumulator) {
      TDigestAccumulator nativeDigest = (TDigestAccumulator) other;
      if (isEmpty() && nativeDigest.hasUnrepresentableBoundaryPayload()) {
        _finiteDigest = nativeDigest.copy();
        mutated();
        return;
      }
    }
    Collection<Centroid> centroids = other.centroids();
    double[] finiteMeans = new double[Math.addExact(centroids.size(), 2)];
    double[] finiteWeights = new double[finiteMeans.length];
    int count = 0;
    double sourceMin = other.getMin();
    double sourceMax = other.getMax();
    double negativeInfinityWeight = 0.0;
    double positiveInfinityWeight = 0.0;
    for (Centroid centroid : centroids) {
      double mean = centroid.mean();
      double weight = centroid.weight();
      if (weight == 0.0) {
        continue;
      }
      if (!(weight > 0.0) || !Double.isFinite(weight) || !(mean >= sourceMin && mean <= sourceMax)) {
        throw new IllegalArgumentException("Invalid TDigest centroid");
      }
      if (mean == Double.NEGATIVE_INFINITY) {
        negativeInfinityWeight = addInfinityWeight(negativeInfinityWeight, weight);
      } else if (mean == Double.POSITIVE_INFINITY) {
        positiveInfinityWeight = addInfinityWeight(positiveInfinityWeight, weight);
      }
      finiteMeans[count] = mean;
      finiteWeights[count++] = weight;
    }
    checkSplittableInfinityWeight(negativeInfinityWeight);
    checkSplittableInfinityWeight(positiveInfinityWeight);
    // Original contributions exclude unit endpoints introduced by boundary repair.
    addInfinityWeight(_negativeInfinityWeight, negativeInfinityWeight);
    addInfinityWeight(_positiveInfinityWeight, positiveInfinityWeight);
    TDigestAccumulator.sortCentroids(finiteMeans, finiteWeights, count);
    count = TDigestAccumulator.normalizeSerializedBoundaries(
        finiteMeans, finiteWeights, count, sourceMin, sourceMax);
    negativeInfinityWeight = 0.0;
    positiveInfinityWeight = 0.0;
    int finiteCount = 0;
    double finiteMin = Double.POSITIVE_INFINITY;
    double finiteMax = Double.NEGATIVE_INFINITY;
    double totalWeight = getTotalWeight();
    for (int i = 0; i < count; i++) {
      double mean = finiteMeans[i];
      double weight = finiteWeights[i];
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
    TDigestAccumulator.checkTotalWeight(totalWeight);
    // Repair may relocate tail mass into a finite header endpoint; merge only the remaining tail mass.
    NonFiniteAwareTDigest preparedReceiver = boundaryPreparedView();
    negativeInfinityWeight = addInfinityWeight(preparedReceiver._negativeInfinityWeight, negativeInfinityWeight);
    positiveInfinityWeight = addInfinityWeight(preparedReceiver._positiveInfinityWeight, positiveInfinityWeight);
    checkSplittableInfinityWeight(negativeInfinityWeight);
    checkSplittableInfinityWeight(positiveInfinityWeight);
    requireMutable();
    if (negativeInfinityWeight > 0.0 || positiveInfinityWeight > 0.0) {
      _finiteDigest.deferBoundaryNormalization();
    }
    try {
      if (finiteCount > 0) {
        finiteMin = Double.isFinite(other.getMin()) ? other.getMin() : finiteMin;
        finiteMax = Double.isFinite(other.getMax()) ? other.getMax() : finiteMax;
        if (other instanceof TDigestAccumulator) {
          _finiteDigest.addHistoricalCentroids(finiteMeans, finiteWeights, finiteCount, finiteMin, finiteMax, false);
        } else {
          _finiteDigest.addCentroids(finiteMeans, finiteWeights, finiteCount, finiteMin, finiteMax, false);
        }
      }
    } catch (RuntimeException | Error e) {
      restoreBoundaryNormalizationAfterFailedMerge();
      throw e;
    }
    if (!(other instanceof TDigestAccumulator)) {
      for (Centroid centroid : centroids) {
        if (Double.isInfinite(centroid.mean())) {
          _finiteDigest.recordFreshFractionalMean(centroid.mean(), centroid.weight());
        }
      }
    }
    _finiteDigest.inheritHistoricalFractionalBoundaries(other);
    _negativeInfinityWeight = negativeInfinityWeight;
    _positiveInfinityWeight = positiveInfinityWeight;
    mutated();
  }

  private static double addInfinityWeight(double currentWeight, double addedWeight) {
    double totalWeight = currentWeight + addedWeight;
    if (currentWeight > 0.0 && addedWeight > 0.0
        && (totalWeight == currentWeight || totalWeight == addedWeight)) {
      throw new IllegalArgumentException("TDigest infinity-tail weight addition would absorb positive mass");
    }
    return totalWeight;
  }

  private void requireMutable() {
    checkMutableInfinityTails();
    if (!hasValidStatistics()) {
      throw TDigestAccumulator.corruptedMutation();
    }
    if (_negativeInfinityWeight == 0.0 && _positiveInfinityWeight == 0.0) {
      _finiteDigest.prepareForMutation();
    } else {
      prepareHistoricalBoundaries();
    }
  }

  private void checkMutableInfinityTails() {
    if (_unrepresentableInfinityTail) {
      throw new IllegalArgumentException(
          "Cannot normalize historical TDigest infinity-tail weight without losing mass");
    }
  }

  private void prepareHistoricalBoundaries() {
    checkMutableInfinityTails();
    if (!_pendingHistoricalBoundaryRepair) {
      return;
    }
    double encodedMin = getMin();
    double encodedMax = getMax();
    List<Centroid> centroids = getFiniteCentroids();
    double[] means = new double[Math.addExact(centroids.size(), 4)];
    double[] weights = new double[means.length];
    int count = 0;
    if (_negativeInfinityWeight > 0.0) {
      means[count] = Double.NEGATIVE_INFINITY;
      weights[count++] = _negativeInfinityWeight;
    }
    for (Centroid centroid : centroids) {
      means[count] = centroid.mean();
      weights[count++] = centroid.weight();
    }
    if (_positiveInfinityWeight > 0.0) {
      means[count] = Double.POSITIVE_INFINITY;
      weights[count++] = _positiveInfinityWeight;
    }
    count = TDigestAccumulator.normalizeSerializedBoundaries(means, weights, count, encodedMin, encodedMax);
    int finiteCount = 0;
    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;
    double negativeInfinityWeight = 0.0;
    double positiveInfinityWeight = 0.0;
    for (int i = 0; i < count; i++) {
      if (means[i] == Double.NEGATIVE_INFINITY) {
        negativeInfinityWeight += weights[i];
      } else if (means[i] == Double.POSITIVE_INFINITY) {
        positiveInfinityWeight += weights[i];
      } else {
        means[finiteCount] = means[i];
        weights[finiteCount++] = weights[i];
        min = Math.min(min, means[i]);
        max = Math.max(max, means[i]);
      }
    }
    TDigestAccumulator finiteDigest = TDigestAccumulator.forLegacyAggregation(compression());
    finiteDigest.deferBoundaryNormalization();
    min = Double.isFinite(encodedMin) ? encodedMin : min;
    max = Double.isFinite(encodedMax) ? encodedMax : max;
    finiteDigest.addHistoricalCentroids(means, weights, finiteCount, min, max, true);
    finiteDigest.inheritHistoricalFractionalBoundaries(_finiteDigest);
    _finiteDigest = finiteDigest;
    _negativeInfinityWeight = negativeInfinityWeight;
    _positiveInfinityWeight = positiveInfinityWeight;
    _pendingHistoricalBoundaryRepair = false;
    invalidateDerivedCaches();
  }

  private NonFiniteAwareTDigest boundaryPreparedView() {
    if (!_pendingHistoricalBoundaryRepair) {
      return this;
    }
    NonFiniteAwareTDigest prepared = copy();
    prepared.prepareHistoricalBoundaries();
    return prepared;
  }

  private void restoreBoundaryNormalizationAfterFailedMerge() {
    invalidateDerivedCaches();
    if (_negativeInfinityWeight == 0.0 && _positiveInfinityWeight == 0.0) {
      _finiteDigest.resetBoundaryNormalization();
    }
  }

  @Nullable
  TDigestAccumulator finiteDigestIfNoTails() {
    if (_negativeInfinityWeight == 0.0 && _positiveInfinityWeight == 0.0) {
      invalidateDerivedCaches();
      return _finiteDigest;
    }
    return null;
  }

  private void mutated() {
    _originalBytes = null;
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
  double getFreshFractionalBoundaryMean(boolean lowerBoundary) {
    return _finiteDigest.getFreshFractionalBoundaryMean(lowerBoundary);
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
    if (_originalBytes != null) {
      return _originalBytes.length;
    }
    if (_negativeInfinityWeight == 0.0 && _positiveInfinityWeight == 0.0) {
      return _finiteDigest.maxSerializedByteSize();
    }
    int finiteByteSize = _finiteDigest.maxSerializedByteSize();
    TDigestAccumulator finiteDigest = _finiteDigest;
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
    if (_pendingHistoricalBoundaryRepair) {
      return _historicalMin;
    }
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
    if (_pendingHistoricalBoundaryRepair) {
      return _historicalMax;
    }
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
      // Centroid reads use the native working-compression view; only serialization applies public compression.
      _finiteCentroids = new ArrayList<>(_finiteDigest.centroids());
    }
    return _finiteCentroids;
  }

  private List<Centroid> getAllCentroids() {
    List<Centroid> finiteCentroids = getFiniteCentroids();
    boolean hasFiniteValues = _finiteDigest.getTotalWeight() > 0.0;
    List<Centroid> centroids = new ArrayList<>(finiteCentroids.size() + 6);
    if (_unrepresentableInfinityTail) {
      if (_negativeInfinityWeight > 0.0) {
        centroids.add(new Centroid(Double.NEGATIVE_INFINITY, _negativeInfinityWeight));
      }
      centroids.addAll(finiteCentroids);
      if (_positiveInfinityWeight > 0.0) {
        centroids.add(new Centroid(Double.POSITIVE_INFINITY, _positiveInfinityWeight));
      }
      return centroids;
    }
    appendInfinityCentroids(centroids, Double.NEGATIVE_INFINITY, _negativeInfinityWeight, true,
        !hasFiniteValues && _positiveInfinityWeight == 0L);
    centroids.addAll(finiteCentroids);
    appendInfinityCentroids(centroids, Double.POSITIVE_INFINITY, _positiveInfinityWeight,
        !hasFiniteValues && _negativeInfinityWeight == 0L, true);
    return centroids;
  }

  @Override
  public byte[] serialize() {
    if (_originalBytes != null) {
      return _originalBytes.clone();
    }
    if (_pendingHistoricalBoundaryRepair) {
      return boundaryPreparedView().serialize();
    }
    if (_negativeInfinityWeight == 0.0 && _positiveInfinityWeight == 0.0) {
      byte[] bytes = _finiteDigest.serialize();
      invalidateDerivedCaches();
      return bytes;
    }
    // Repair boundaries only after enclosing infinity tails establish the complete distribution's extrema.
    _finiteDigest.compress();
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
    centroidCount = TDigestAccumulator.normalizeSerializedBoundaries(
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
    checkSplittableInfinityWeight(weight);
    int count = 0;
    if (unitWeightAtStart && weight >= (unitWeightAtEnd ? 2.0 : 1.0)) {
      appendInfinityCentroid(centroids, means, weights, offset + count++, value, 1.0);
      weight = TDigestAccumulator.checkedResidualWeight(weight, 1.0);
    }
    boolean appendUnitWeightAtEnd = unitWeightAtEnd && weight >= 1.0;
    if (appendUnitWeightAtEnd) {
      weight = TDigestAccumulator.checkedResidualWeight(weight, 1.0);
    }
    if (weight > 0.0) {
      appendInfinityCentroid(centroids, means, weights, offset + count++, value, weight);
    }
    if (appendUnitWeightAtEnd) {
      appendInfinityCentroid(centroids, means, weights, offset + count++, value, 1.0);
    }
    return count;
  }

  private static void checkSplittableInfinityWeight(double weight) {
    if (weight >= 1.0) {
      TDigestAccumulator.checkedResidualWeight(weight, 1.0);
    }
  }

  private static boolean canSplitInfinityWeight(double weight) {
    try {
      checkSplittableInfinityWeight(weight);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
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
}
