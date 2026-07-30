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
package org.apache.pinot.calcite.plan;

import java.util.Objects;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;


/// Pinot-specific implementation of [RelOptCost] for the multi-stage query engine (MSE).
///
/// ### Ordering semantics (rows-dominated lexicographic)
/// Cost comparison uses a strict (rows, cpu, io) lexicographic order:
/// 1. Row count is the primary key — plans that process fewer rows are cheaper.
/// 1. CPU cost breaks ties when row counts are equal.
/// 1. IO cost breaks ties when both rows and CPU are equal.
///
/// ### Deliberate difference from Calcite's `VolcanoCost`
/// Calcite's `VolcanoCost.isLe` compares *only* row count: two costs with the same
/// number of rows are considered equal regardless of their cpu or io values. This means
/// `VolcanoCost` has no way to break ties when row counts match, which can leave join-order
/// choices arbitrary.
///
/// Pinot's MSE cost model instead applies a full lexicographic tie-break: equal rows →
/// compare cpu; equal rows+cpu → compare io. This gives the planner a deterministic total order
/// over all finite cost triples and lets CPU/IO estimates (however coarse) influence join ordering
/// when row counts happen to match.
///
/// ### Thread safety
/// Instances are immutable; all fields are set in the constructor and never modified.
/// The class is therefore inherently thread-safe.
public class PinotRelOptCost implements RelOptCost {

  /// Epsilon used by [#isEqWithEpsilon] — mirrors `VolcanoCost`'s value.
  static final double EPSILON = 1.0e-5;

  /// Cost representing an infeasible / infinite plan.
  public static final PinotRelOptCost INFINITY =
      new PinotRelOptCost(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY) {
        @Override
        public String toString() {
          return "{inf}";
        }
      };

  /// A very large but finite cost (uses `Double.MAX_VALUE` for each component),
  /// analogous to `VolcanoCost.HUGE`.
  public static final PinotRelOptCost HUGE =
      new PinotRelOptCost(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE) {
        @Override
        public String toString() {
          return "{huge}";
        }
      };

  /// Zero cost — the cheapest possible plan.
  public static final PinotRelOptCost ZERO = new PinotRelOptCost(0.0, 0.0, 0.0) {
    @Override
    public String toString() {
      return "{0}";
    }
  };

  /// A very small but non-zero cost (rows=1, cpu=1, io=0), analogous to `VolcanoCost.TINY`.
  public static final PinotRelOptCost TINY = new PinotRelOptCost(1.0, 1.0, 0.0) {
    @Override
    public String toString() {
      return "{tiny}";
    }
  };

  private final double _rows;
  private final double _cpu;
  private final double _io;

  /// Constructs a `PinotRelOptCost` with the given component values.
  ///
  /// @param rows estimated number of output rows (primary ordering key)
  /// @param cpu  estimated CPU cost (first tiebreaker)
  /// @param io   estimated IO cost (second tiebreaker)
  public PinotRelOptCost(double rows, double cpu, double io) {
    _rows = rows;
    _cpu = cpu;
    _io = io;
  }

  @Override
  public double getRows() {
    return _rows;
  }

  @Override
  public double getCpu() {
    return _cpu;
  }

  @Override
  public double getIo() {
    return _io;
  }

  @Override
  public boolean isInfinite() {
    return this == INFINITY
        || Double.isInfinite(_rows)
        || Double.isInfinite(_cpu)
        || Double.isInfinite(_io);
  }

  /// Returns `true` if this cost is less than or equal to `other` under the
  /// rows-dominated lexicographic order: rows first, then cpu, then io.
  ///
  /// This differs from `VolcanoCost.isLe`, which returns `true` whenever
  /// `this.rows <= other.rows`, ignoring cpu and io entirely.
  @Override
  public boolean isLe(RelOptCost other) {
    PinotRelOptCost that = (PinotRelOptCost) other;
    if (this == that) {
      return true;
    }
    if (_rows < that._rows) {
      return true;
    }
    if (_rows > that._rows) {
      return false;
    }
    // rows equal — break tie on cpu
    if (_cpu < that._cpu) {
      return true;
    }
    if (_cpu > that._cpu) {
      return false;
    }
    // rows+cpu equal — break tie on io
    return _io <= that._io;
  }

  /// Returns `true` if this cost is strictly less than `other`.
  /// Defined as `isLe(other) && !equals(other)`.
  @Override
  public boolean isLt(RelOptCost other) {
    return isLe(other) && !equals(other);
  }

  /// Returns `true` if all three components are exactly equal (bitwise double equality).
  /// For approximate equality see [#isEqWithEpsilon].
  @Override
  public boolean equals(RelOptCost other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof PinotRelOptCost)) {
      return false;
    }
    PinotRelOptCost that = (PinotRelOptCost) other;
    return Double.compare(_rows, that._rows) == 0
        && Double.compare(_cpu, that._cpu) == 0
        && Double.compare(_io, that._io) == 0;
  }

  /// Returns `true` if all three components are within [#EPSILON] of each other.
  /// Mirrors the epsilon used by `VolcanoCost` (1e-5).
  @Override
  public boolean isEqWithEpsilon(RelOptCost other) {
    if (!(other instanceof PinotRelOptCost)) {
      return false;
    }
    PinotRelOptCost that = (PinotRelOptCost) other;
    if (this == that) {
      return true;
    }
    return Math.abs(_rows - that._rows) < EPSILON
        && Math.abs(_cpu - that._cpu) < EPSILON
        && Math.abs(_io - that._io) < EPSILON;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PinotRelOptCost) {
      return equals((RelOptCost) obj);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_rows, _cpu, _io);
  }

  /// Returns the component-wise sum. If either operand is [#INFINITY], returns
  /// [#INFINITY] — mirroring `VolcanoCost.plus`.
  @Override
  public RelOptCost plus(RelOptCost other) {
    PinotRelOptCost that = (PinotRelOptCost) other;
    if (this == INFINITY || that == INFINITY) {
      return INFINITY;
    }
    return new PinotRelOptCost(_rows + that._rows, _cpu + that._cpu, _io + that._io);
  }

  /// Returns the component-wise difference. If `this` is [#INFINITY], returns
  /// [#INFINITY] — mirroring `VolcanoCost.minus`.
  @Override
  public RelOptCost minus(RelOptCost other) {
    if (this == INFINITY) {
      return this;
    }
    PinotRelOptCost that = (PinotRelOptCost) other;
    return new PinotRelOptCost(_rows - that._rows, _cpu - that._cpu, _io - that._io);
  }

  /// Returns this cost scaled by `factor`. If `this` is [#INFINITY], returns
  /// [#INFINITY] — mirroring `VolcanoCost.multiplyBy`.
  @Override
  public RelOptCost multiplyBy(double factor) {
    if (this == INFINITY) {
      return this;
    }
    return new PinotRelOptCost(_rows * factor, _cpu * factor, _io * factor);
  }

  /// Returns the geometric mean of non-trivial (non-zero, non-infinite) per-component ratios of
  /// `this / other`, exactly mirroring `VolcanoCost.divideBy`.
  ///
  /// Each component contributes to the product only when both `this` and `other`
  /// have a non-zero, non-infinite value for that component; the exponent equals the number of
  /// contributing components.
  @Override
  public double divideBy(RelOptCost other) {
    PinotRelOptCost that = (PinotRelOptCost) other;
    double d = 1.0;
    double n = 0.0;
    if (_rows != 0.0 && !Double.isInfinite(_rows)
        && that._rows != 0.0 && !Double.isInfinite(that._rows)) {
      d *= _rows / that._rows;
      n++;
    }
    if (_cpu != 0.0 && !Double.isInfinite(_cpu)
        && that._cpu != 0.0 && !Double.isInfinite(that._cpu)) {
      d *= _cpu / that._cpu;
      n++;
    }
    if (_io != 0.0 && !Double.isInfinite(_io)
        && that._io != 0.0 && !Double.isInfinite(that._io)) {
      d *= _io / that._io;
      n++;
    }
    if (n == 0.0) {
      return 1.0;
    }
    return Math.pow(d, 1.0 / n);
  }

  @Override
  public String toString() {
    return "{rows: " + _rows + ", cpu: " + _cpu + ", io: " + _io + "}";
  }

  // ---------------------------------------------------------------------------
  // Inner factory
  // ---------------------------------------------------------------------------

  /// [RelOptCostFactory] that creates [PinotRelOptCost] instances.
  ///
  /// Use [#INSTANCE] to avoid repeated allocations of the factory itself.
  public static final class Factory implements RelOptCostFactory {

    /// Singleton instance.
    public static final Factory INSTANCE = new Factory();

    private Factory() {
    }

    @Override
    public RelOptCost makeCost(double rowCount, double cpu, double io) {
      return new PinotRelOptCost(rowCount, cpu, io);
    }

    @Override
    public RelOptCost makeHugeCost() {
      return HUGE;
    }

    @Override
    public RelOptCost makeInfiniteCost() {
      return INFINITY;
    }

    @Override
    public RelOptCost makeTinyCost() {
      return TINY;
    }

    @Override
    public RelOptCost makeZeroCost() {
      return ZERO;
    }
  }
}
