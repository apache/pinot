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
package org.apache.pinot.segment.spi.customobject;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

/// Mutable percentile digest contract for aggregation plugins and legacy t-digest byte encodings.
/// Implementations are not thread-safe; callers must serialize access to each aggregation state.
public abstract class TDigest {
  public abstract void add(double value);

  public void add(double value, int weight) {
    add(value, (double) weight);
  }

  /// Adds positive finite mass, rejecting a valid distribution's total that would overflow double.
  public abstract void add(double value, double weight);

  public abstract void add(TDigest other);

  public abstract void add(List<? extends TDigest> others);

  public abstract void compress();

  /// Returns the legacy long size view, saturating when the precise mass exceeds its range.
  public abstract long size();

  /// Returns centroid mass without narrowing fractional or large weights. Valid totals must remain finite;
  /// add and merge throw IllegalArgumentException when the summed mass exceeds the finite double range.
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

  public abstract int byteSize();

  /// Bounds bytes written by asBytes for the current state; conservative bounds may include verbose expansion.
  /// Mutable implementations should override this without flushing buffered inputs; the default asks byteSize
  /// and may perform compression.
  public int maxSerializedByteSize() {
    return byteSize();
  }

  /// Returns space for asSmallBytes; degraded legacy state can retain its original verbose encoding.
  /// Implementations fail explicitly when compact centroid counts or capacities cannot fit their signed short fields.
  public abstract int smallByteSize();

  public abstract void asBytes(ByteBuffer buffer);

  /// Writes compact bytes when possible, retaining verbose bytes for degraded legacy state.
  public abstract void asSmallBytes(ByteBuffer buffer);

  public abstract int centroidCount();

  public abstract double getMin();

  public abstract double getMax();

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
