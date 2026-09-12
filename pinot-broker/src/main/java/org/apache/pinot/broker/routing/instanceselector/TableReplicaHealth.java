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
package org.apache.pinot.broker.routing.instanceselector;

import javax.annotation.concurrent.Immutable;


/// Table-level view of how well a table's segments are replicated across the servers this broker can
/// actually route to: one set of numbers per table, aggregated over its segments. Computed alongside
/// [SegmentStates] and published as a whole so the values stay mutually consistent. The percentage is
/// `servingReplicas * 100 / expectedReplicas` evaluated per segment and then minimised, so that a table
/// whose segments are not uniformly replicated still reads correctly, and counted so the minimum comes with
/// the number of segments behind it.
///
/// A snapshot, not a history: a segment is reported the moment it drops, and debouncing belongs in the
/// alert. Only two populations are left out - single-replica segments, and segments routing still calls new
/// (it does not serve those either, so a stuck push is invisible here by design). Consuming segments count.
@Immutable
public class TableReplicaHealth {
  /// Value reported when a table has no segments to measure.
  public static final int FULLY_REPLICATED_PERCENT = 100;

  /// Assigned replicas below which a segment is not measured: a single-replica segment has no redundancy even
  /// when healthy, so measuring it would report the replication level rather than an incident.
  private static final int MIN_MEASURED_REPLICAS = 2;

  private final int _minPercentOfReplicas;
  private final int _numSegmentsAtMinPercentOfReplicas;
  private final int _numUnavailableSegments;

  public TableReplicaHealth(int minPercentOfReplicas, int numSegmentsAtMinPercentOfReplicas,
      int numUnavailableSegments) {
    _minPercentOfReplicas = minPercentOfReplicas;
    _numSegmentsAtMinPercentOfReplicas = numSegmentsAtMinPercentOfReplicas;
    _numUnavailableSegments = numUnavailableSegments;
  }

  /// Returns the worst measured segment's replica percentage, or [#FULLY_REPLICATED_PERCENT] if there are
  /// none. The minimum, so one unservable segment is not diluted by a large healthy table.
  public int getMinPercentOfReplicas() {
    return _minPercentOfReplicas;
  }

  /// Returns how many measured segments sit at [#getMinPercentOfReplicas] - the blast radius behind it, which
  /// the minimum alone cannot distinguish: one straggler and a whole table down read the same. `0` only when
  /// there is nothing to measure, since otherwise some segment is always the worst one.
  public int getNumSegmentsAtMinPercentOfReplicas() {
    return _numSegmentsAtMinPercentOfReplicas;
  }

  /// Returns how many segments cannot be routed anywhere, whatever their replication. Matches
  /// [SegmentStates#getUnavailableSegments()], i.e. what the query path refuses to serve, so unlike the two
  /// above it includes single-replica segments. When [#getMinPercentOfReplicas] is `0`, this is how much of
  /// the data is already gone rather than merely degraded.
  public int getNumUnavailableSegments() {
    return _numUnavailableSegments;
  }

  /// Returns whether a segment's replication is high enough to measure at all.
  static boolean shouldMeasure(int expectedReplicas) {
    return expectedReplicas >= MIN_MEASURED_REPLICAS;
  }

  /// Returns the percentage of assigned replicas that are serving, truncated and capped at 100.
  static int toPercent(int servingReplicas, int expectedReplicas) {
    return Math.min(FULLY_REPLICATED_PERCENT, servingReplicas * FULLY_REPLICATED_PERCENT / expectedReplicas);
  }
}
