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

public class InstanceSelectorConfig {
  private final boolean _useFixedReplica;
  private final long _newSegmentExpirationTimeInSeconds;
  private final boolean _emitSinglePoolSegmentsMetrics;
  private final boolean _emitReplicaHealthMetrics;

  public InstanceSelectorConfig(boolean useFixedReplica, long newSegmentExpirationTimeInSeconds,
      boolean emitSinglePoolSegmentsMetrics) {
    this(useFixedReplica, newSegmentExpirationTimeInSeconds, emitSinglePoolSegmentsMetrics, true);
  }

  public InstanceSelectorConfig(boolean useFixedReplica, long newSegmentExpirationTimeInSeconds,
      boolean emitSinglePoolSegmentsMetrics, boolean emitReplicaHealthMetrics) {
    _useFixedReplica = useFixedReplica;
    _newSegmentExpirationTimeInSeconds = newSegmentExpirationTimeInSeconds;
    _emitSinglePoolSegmentsMetrics = emitSinglePoolSegmentsMetrics;
    _emitReplicaHealthMetrics = emitReplicaHealthMetrics;
  }

  public boolean isUseFixedReplica() {
    return _useFixedReplica;
  }

  public long getNewSegmentExpirationTimeInSeconds() {
    return _newSegmentExpirationTimeInSeconds;
  }

  public boolean shouldEmitSinglePoolSegmentsMetrics() {
    return _emitSinglePoolSegmentsMetrics;
  }

  /// Whether this selector should report the table's replica health gauges.
  ///
  /// The replica health gauges are keyed by table name, so only the selector that covers all of the
  /// table's segments may report them. A selector built for a table sampler sees a sampled subset of
  /// the segments and must not, or it would overwrite the table's values with numbers measured over
  /// that sample.
  public boolean shouldEmitReplicaHealthMetrics() {
    return _emitReplicaHealthMetrics;
  }
}
