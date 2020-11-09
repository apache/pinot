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

import javax.annotation.Nullable;
import org.apache.pinot.core.data.manager.OfflineSegmentFetcherAndLoader;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;


/**
 * Segment data manager for immutable segment.
 */
public class OfflineSegmentDataManager extends SegmentDataManager {

  private ImmutableSegment _immutableSegment;
  private String _segmentName;
  private IndexLoadingConfig _indexLoadingConfig;
  OfflineSegmentFetcherAndLoader _fetcherAndLoader;
  private String _tableNameWithType;
  private SegmentCacheManager _cacheManager;

  @Override
  public int hashCode() {
    return getSegmentName().hashCode() + getTableNameWithType().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj.getClass().isInstance(OfflineSegmentDataManager.class)) {
      OfflineSegmentDataManager other = (OfflineSegmentDataManager) obj;
      return getTableNameWithType().equals(other.getTableNameWithType()) && getSegmentName()
          .equals(other.getSegmentName());
    }

    return false;
  }

  @Override
  public boolean hasLocalData() {
    return _immutableSegment != null;
  }

  public OfflineSegmentDataManager(String tableNameWithType, String segmentName,
      OfflineSegmentFetcherAndLoader fetcherAndLoader, @Nullable SegmentCacheManager cacheManager,
      IndexLoadingConfig indexLoadingConfig) {
    _tableNameWithType = tableNameWithType;
    _cacheManager = cacheManager;
    _fetcherAndLoader = fetcherAndLoader;
    _segmentName = segmentName;
    _indexLoadingConfig = indexLoadingConfig;
    if (cacheManager == null) { // Must be eager loading
      _immutableSegment = _fetcherAndLoader.fetchAndLoadOfflineSegment(_tableNameWithType, _segmentName, _indexLoadingConfig);
    }
  }

  @Override
  public String getSegmentName() {
    return _segmentName;
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public ImmutableSegment loadSegmentIntoCache() {
    ImmutableSegment segment =
        _fetcherAndLoader.fetchAndLoadOfflineSegment(_tableNameWithType, _segmentName, _indexLoadingConfig);
    _cacheManager.put(this, segment);
    return segment;
  }

  @Override
  public ImmutableSegment getSegment() {
    if (_cacheManager == null) {
      return _immutableSegment;
    }

    ImmutableSegment cached = _cacheManager.get(this);
    if (cached == null) {
      cached = loadSegmentIntoCache();
    }
    return cached;
  }

  public void releaseSegment() {
    _fetcherAndLoader.deleteOfflineSegment(_tableNameWithType, _segmentName);
  }

  @Override
  public void destroy() {
    _immutableSegment.destroy();
  }

  @Override
  public String toString() {
    return "ImmutableSegmentDataManager(" + _immutableSegment.getSegmentName() + ")";
  }
}
