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
import java.util.Collection;
import java.util.List;

/// Pinot's mutable t-digest contract using the existing K1 aggregation behavior and legacy byte encodings.
///
/// Implementations are not safe for concurrent mutation; callers must serialize access to each aggregation state.
public abstract class TDigest {
  public abstract void add(double value);

  public abstract void add(double value, int weight);

  public abstract void add(TDigest other);

  public abstract void add(List<? extends TDigest> others);

  public abstract void compress();

  public abstract long size();

  /// Returns centroid mass without narrowing fractional or large weights to the legacy long size view.
  public double getTotalWeight() {
    return size();
  }

  public abstract double cdf(double value);

  public abstract double quantile(double quantile);

  public abstract Collection<Centroid> centroids();

  public abstract double compression();

  public abstract int byteSize();

  public abstract int smallByteSize();

  public abstract void asBytes(ByteBuffer buffer);

  public abstract void asSmallBytes(ByteBuffer buffer);

  public abstract int centroidCount();

  public abstract double getMin();

  public abstract double getMax();

  /// Immutable centroid view. Counts retain the legacy int API; serialization preserves double weights.
  public record Centroid(double mean, int count) {
  }
}
