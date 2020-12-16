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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Weigher;
import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentCacheManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentCacheManager.class);
  private InstanceDataManager _instanceDataManager;

  private final Weigher<OfflineSegmentDataManager, ImmutableSegment> segmentSizeInMb =
      (OfflineSegmentDataManager segmentDataManager, ImmutableSegment segment) -> {
        File indexDir = new File(getSegmentLocalDirectory(segmentDataManager));
        long fileSizeBytes = FileUtils.sizeOfDirectory(indexDir);
        return (int) Math.ceil((double) fileSizeBytes / FileUtils.ONE_MB);
      };
  private final RemovalListener<OfflineSegmentDataManager, ImmutableSegment> segmentRemovalListener =
      (OfflineSegmentDataManager segmentDataManager, ImmutableSegment segment, RemovalCause cause) -> {
        LOGGER.info("Evicting segment {} of table {} with cause {}", segmentDataManager.getSegmentName(),
            segmentDataManager.getTableNameWithType(), cause.toString());
        segmentDataManager.releaseSegment();
      };

  private Cache<OfflineSegmentDataManager, ImmutableSegment> _lazyLoadedSegmentCache;

  public SegmentCacheManager(InstanceDataManager instanceDataManager) {
    _instanceDataManager = instanceDataManager;
    this.initCache();
  }

  private void initCache() {
    int maxDiscUsage = _instanceDataManager.maxSegmentDiscUsageMb();
    ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    Scheduler scheduler = Scheduler.forScheduledExecutorService(scheduledExecutorService);
    LOGGER.info("Creating segment cache with max disc usage {}Mb", maxDiscUsage);
    _lazyLoadedSegmentCache = Caffeine.newBuilder()
        .maximumWeight(maxDiscUsage)
        .weigher(segmentSizeInMb)
        .removalListener(segmentRemovalListener)
        .scheduler(scheduler)
        .build();
  }

  public String getSegmentLocalDirectory(OfflineSegmentDataManager segmentIdentifer) {
    return _instanceDataManager.getSegmentDataDirectory() + "/" + segmentIdentifer.getTableNameWithType() + "/"
        + segmentIdentifer.getSegmentName();
  }

  public void put(OfflineSegmentDataManager segmentDataManager, ImmutableSegment segment) {
    _lazyLoadedSegmentCache.put(segmentDataManager, segment);
  }

  @Nullable
  public ImmutableSegment get(OfflineSegmentDataManager segmentDataManager) {
    return _lazyLoadedSegmentCache.getIfPresent(segmentDataManager);
  }
}
