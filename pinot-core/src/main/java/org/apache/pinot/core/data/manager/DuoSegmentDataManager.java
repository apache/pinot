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
package org.apache.pinot.core.data.manager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * Segment data manager tracking two segments associated with one segment name, e.g. when committing a mutable
 * segment, a new immutable segment is created to replace the mutable one, and the two segments are having same name.
 * By tracked both with this segment data manager, we can provide queries both segments for complete data view.
 * The primary segment represents all segments tracked by this manager for places asking for segment metadata.
 */
public class DuoSegmentDataManager extends SegmentDataManager {
  private final SegmentDataManager _primary;
  private final List<SegmentDataManager> _segmentDataManagers;

  public DuoSegmentDataManager(SegmentDataManager primary, SegmentDataManager secondary) {
    _primary = primary;
    _segmentDataManagers = Arrays.asList(_primary, secondary);
  }

  @Override
  public long getLoadTimeMs() {
    return _primary.getLoadTimeMs();
  }

  @Override
  public synchronized int getReferenceCount() {
    return _primary.getReferenceCount();
  }

  @Override
  public String getSegmentName() {
    return _primary.getSegmentName();
  }

  @Override
  public IndexSegment getSegment() {
    return _primary.getSegment();
  }

  @Override
  public synchronized boolean increaseReferenceCount() {
    boolean any = false;
    for (SegmentDataManager segmentDataManager : _segmentDataManagers) {
      if (segmentDataManager.increaseReferenceCount()) {
        any = true;
      }
    }
    return any;
  }

  @Override
  public synchronized boolean decreaseReferenceCount() {
    boolean any = false;
    for (SegmentDataManager segmentDataManager : _segmentDataManagers) {
      if (segmentDataManager.decreaseReferenceCount()) {
        any = true;
      }
    }
    return any;
  }

  @Override
  public boolean hasMultiSegments() {
    return true;
  }

  @Override
  public List<IndexSegment> getSegments() {
    List<IndexSegment> segments = new ArrayList<>(_segmentDataManagers.size());
    for (SegmentDataManager segmentDataManager : _segmentDataManagers) {
      if (segmentDataManager.getReferenceCount() > 0) {
        segments.add(segmentDataManager.getSegment());
      }
    }
    return segments;
  }

  @Override
  public void doOffload() {
    for (SegmentDataManager segmentDataManager : _segmentDataManagers) {
      if (segmentDataManager.getReferenceCount() == 0) {
        segmentDataManager.offload();
      }
    }
  }

  @Override
  protected void doDestroy() {
    for (SegmentDataManager segmentDataManager : _segmentDataManagers) {
      if (segmentDataManager.getReferenceCount() == 0) {
        segmentDataManager.destroy();
      }
    }
  }
}
