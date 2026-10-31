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


/// Segment data manager for immutable segment.
public class ImmutableSegmentDataManager extends SegmentDataManager {

  private final ImmutableSegment _immutableSegment;
  private volatile boolean _metadataReady;

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

  /// Marks this segment's partition metadata as initialized after a successful add, preload, or replacement.
  public void markMetadataReady() {
    _metadataReady = true;
  }

  /// Returns whether this instance has finished initializing its partition metadata. Registration alone is not enough:
  /// a new upsert segment is published before its metadata finishes loading. Callers must look up the currently
  /// registered manager because this flag does not carry over to a replacement instance.
  public boolean isMetadataReady() {
    return _metadataReady;
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
