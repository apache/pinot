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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentCacheManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentCacheManager.class);
  private InstanceDataManager _instanceDataManager;

  class SegmentIdentifer {
    private final String _tableNameWithType;
    private final String _segmentName;

    @Override
    public int hashCode() {
      return getSegmentName().hashCode() + getTableNameWithType().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj.getClass().isInstance(SegmentIdentifer.class)) {
        SegmentIdentifer other = (SegmentIdentifer) obj;
        return getTableNameWithType().equals(other.getTableNameWithType()) && getSegmentName()
            .equals(other.getSegmentName());
      }

      return false;
    }

    public SegmentIdentifer(String tableNameWithType, String segmentName) {
      _tableNameWithType = tableNameWithType;
      _segmentName = segmentName;
    }

    public String getSegmentName() {
      return _segmentName;
    }

    public String getTableNameWithType() {
      return _tableNameWithType;
    }
  }

  private final Weigher<SegmentIdentifer, OfflineSegmentDataManager> segmentSizeInMb =
      (SegmentIdentifer identifier, OfflineSegmentDataManager segment) -> {
        File indexDir = new File(getSegmentLocalDirectory(identifier));
        long fileSizeBytes = FileUtils.sizeOfDirectory(indexDir);
        return (int) Math.ceil((double) fileSizeBytes / FileUtils.ONE_MB);
      };
  private final RemovalListener<SegmentIdentifer, OfflineSegmentDataManager> segmentRemovalListener =
      (SegmentIdentifer identifier, OfflineSegmentDataManager segmentManager, RemovalCause cause) -> {
        LOGGER.info("Evicting segment {} of table {} with cause {}", segmentManager.getSegmentName(),
            identifier.getTableNameWithType(), cause.toString());
        segmentManager.releaseSegment();
      };

  private Cache<SegmentIdentifer, OfflineSegmentDataManager> _lazyLoadedSegmentCache;

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

  public String getSegmentLocalDirectory(SegmentIdentifer segmentIdentifer) {
    return _instanceDataManager.getSegmentDataDirectory() + "/" + segmentIdentifer.getTableNameWithType() + "/"
        + segmentIdentifer.getSegmentName();
  }

  public void register(OfflineSegmentDataManager segmentDataManager) {
    _lazyLoadedSegmentCache
        .put(new SegmentIdentifer(segmentDataManager.getTableNameWithType(), segmentDataManager.getSegmentName()),
            segmentDataManager);
  }
}
