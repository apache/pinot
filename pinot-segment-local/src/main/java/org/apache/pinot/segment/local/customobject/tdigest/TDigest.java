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

import java.util.Collection;
import java.util.List;

/// Mutable percentile digest base for Pinot aggregation and legacy t-digest byte encodings.
/// Implementations are not thread-safe; callers must serialize access to each aggregation state.
/// Historical corrupted payloads may be retained unchanged, but cannot be mutated or mixed with healthy state.
/// Fractional mass is preserved; t-digest 3.3 can reject endpoint weights below one or singleton masses between
/// one and two during recompression because its endpoint invariant requires unit weights.
public abstract class TDigest {
  public abstract void add(double value);

  public void add(double value, int weight) {
    add(value, (double) weight);
  }

  /// Adds positive finite mass, rejecting a valid distribution's total that would overflow double.
  /// Fractional interior mass is supported. Serialization rejects fresh endpoint mass below one, or a singleton
  /// mass between one and two, because legacy readers require unit endpoints. For example, `add(5, 0.5)` is
  /// accepted but cannot be serialized alone; subsequent input can make that mass interior or representable.
  public abstract void add(double value, double weight);

  /// Merges an exclusively owned source; implementations may compress that mutable source in place.
  public abstract void add(TDigest other);

  public void add(List<? extends TDigest> others) {
    for (TDigest other : others) {
      add(other);
    }
  }

  public abstract void compress();

  /// Returns the legacy long size view, saturating when the precise mass exceeds its range.
  public long size() {
    return (long) getTotalWeight();
  }

  /// Returns centroid mass without narrowing fractional or large weights. Valid totals must remain finite;
  /// add and merge throw IllegalArgumentException when the summed mass exceeds the finite double range.
  /// An invalid historical payload may report its original negative or non-finite total; it cannot contribute
  /// to another digest.
  public abstract double getTotalWeight();

  /// Returns false when historical numerical corruption leaves the distribution unknown.
  public boolean hasValidStatistics() {
    return true;
  }

  /// An unknown distribution remains present even when its historical weights sum to zero.
  public boolean isEmpty() {
    return getTotalWeight() == 0.0 && hasValidStatistics();
  }

  public abstract double cdf(double value);

  public abstract double quantile(double quantile);

  /// Returns immutable centroid values with their precise double weights.
  /// Reading this view may flush buffered inputs without applying the final public compression.
  public abstract Collection<Centroid> centroids();

  public abstract double compression();

  /// Serializes one independently owned legacy-compatible payload. Implementations own their compression policy.
  /// Unchanged historical encodings may be retained byte-exactly; fresh unrepresentable boundaries are rejected.
  public abstract byte[] serialize();

  /// Bounds independently owned serialization without requiring a two-step size/write protocol.
  /// Mutable implementations can override this to avoid flushing buffered inputs.
  public int maxSerializedByteSize() {
    return serialize().length;
  }

  public abstract int centroidCount();

  public abstract double getMin();

  public abstract double getMax();

  /// Returns the mean of an unsupported fractional endpoint inherited from validated legacy bytes, or NaN when
  /// none is known. The lower or upper boundary is selected by `lowerBoundary`. Producers of fresh values retain
  /// this default; mergers propagate the provenance without granting permission to new fractional extrema.
  double getHistoricalFractionalBoundaryMean(boolean lowerBoundary) {
    return Double.NaN;
  }

  /// Extremal fresh fractional input means, retained across merges to prevent reusing historical permission for
  /// new mass at the same endpoint. NaN means this digest has received no fresh fractional inputs.
  double getFreshFractionalBoundaryMean(boolean lowerBoundary) {
    return Double.NaN;
  }

  /// Immutable centroid value preserving fractional and large mass.
  public static final class Centroid {
    private final double _mean;
    private final double _weight;

    public Centroid(double mean, double weight) {
      _mean = mean;
      _weight = weight;
    }

    public double mean() {
      return _mean;
    }

    public double weight() {
      return _weight;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof Centroid)) {
        return false;
      }
      Centroid centroid = (Centroid) other;
      return Double.doubleToLongBits(_mean) == Double.doubleToLongBits(centroid._mean)
          && Double.doubleToLongBits(_weight) == Double.doubleToLongBits(centroid._weight);
    }

    @Override
    public int hashCode() {
      return 31 * Double.hashCode(_mean) + Double.hashCode(_weight);
    }
  }
}
