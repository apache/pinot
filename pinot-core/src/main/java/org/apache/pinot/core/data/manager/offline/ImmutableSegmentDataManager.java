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
package org.apache.pinot.core.data.manager.offline;

import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.spi.ImmutableSegment;


/// Manages an immutable segment. Segment loading publishes completion of this segment's upsert/dedup metadata
/// initialization through a volatile flag, which consumer threads read before relying on it as partition history.
public class ImmutableSegmentDataManager extends SegmentDataManager {

  private final ImmutableSegment _immutableSegment;
  private volatile boolean _partitionMetadataInitialized;

  public ImmutableSegmentDataManager(ImmutableSegment immutableSegment) {
    _immutableSegment = immutableSegment;
  }

  @Override
  public String getSegmentName() {
    return _immutableSegment.getSegmentName();
  }

  @Override
  public ImmutableSegment getSegment() {
    return _immutableSegment;
  }

  /// Marks this segment's upsert/dedup metadata initialization complete after add, preload, or replacement.
  /// Also called when the table requires no partition metadata.
  public void markPartitionMetadataInitialized() {
    _partitionMetadataInitialized = true;
  }

  /// Returns whether this segment has finished initializing its contribution to the partition's upsert/dedup state.
  /// This does not imply that all partition history is ready. A new upsert segment is registered before initialization
  /// finishes, so registration alone is insufficient. Callers must inspect the currently registered manager because
  /// initialization of an old instance does not establish readiness for its replacement.
  public boolean isPartitionMetadataInitialized() {
    return _partitionMetadataInitialized;
  }

  @Override
  public void doOffload() {
    _immutableSegment.offload();
  }

  @Override
  protected void doDestroy() {
    _immutableSegment.destroy();
  }

  @Override
  public String toString() {
    return "ImmutableSegmentDataManager(" + _immutableSegment.getSegmentName() + ")";
  }
}
