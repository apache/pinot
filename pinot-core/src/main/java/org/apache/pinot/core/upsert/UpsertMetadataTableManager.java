package org.apache.pinot.core.upsert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.core.realtime.impl.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.data.readers.PrimaryKey;


public class UpsertMetadataTableManager {
  private final RealtimeTableDataManager _realtimeTableDataManager;
  // TODO(upsert): used for debugging
  private static boolean _upsertInQueryEnabled = true;


  private final Map<Integer, UpsertMetadataPartitionManager> _partitionMetadataManagerMap = new ConcurrentHashMap();

  public UpsertMetadataTableManager(RealtimeTableDataManager realtimeTableDataManager) {
    _realtimeTableDataManager = realtimeTableDataManager;
  }

  private synchronized UpsertMetadataPartitionManager getOrCreatePartitionManager(int partitionId) {
    if(!_partitionMetadataManagerMap.containsKey(partitionId)) {
      _partitionMetadataManagerMap.put(partitionId, new UpsertMetadataPartitionManager(partitionId));
    }
    return _partitionMetadataManagerMap.get(partitionId);
  }

  public boolean isEmpty() {
    return _partitionMetadataManagerMap.isEmpty();
  }

  public void removeRecordLocation(int partitionId, PrimaryKey primaryKey) {
    getOrCreatePartitionManager(partitionId).removeRecordLocation(primaryKey);
  }

  public boolean containsKey(int partitionId, PrimaryKey primaryKey) {
    return getOrCreatePartitionManager(partitionId).containsKey(primaryKey);
  }

  public RecordLocation getRecordLocation(int partitionId, PrimaryKey primaryKey) {
    return getOrCreatePartitionManager(partitionId).getRecordLocation(primaryKey);
  }

  public void updateRecordLocation(int partitionId,  PrimaryKey primaryKey, RecordLocation recordLocation) {
    getOrCreatePartitionManager(partitionId).updateRecordLocation(primaryKey, recordLocation);
  }

  public ThreadSafeMutableRoaringBitmap getValidDocIndex(int partitionId, String segmentName) {
    // TODO(upsert) check existence of the validDocIndex of the given segment, rebuild it if not available
    return getOrCreatePartitionManager(partitionId).getValidDocIndex(segmentName);
  }

  public void putUpsertMetadataOfPartition(int partitionId, String segmentName, Map<PrimaryKey, RecordLocation> primaryKeyIndex,
      ThreadSafeMutableRoaringBitmap validDocIndex) {
    getOrCreatePartitionManager(partitionId).putUpsertMetadata(segmentName, primaryKeyIndex, validDocIndex);
  }

  public static boolean isUpsertInQueryEnabled() {
    return _upsertInQueryEnabled;
  }

  public static void enableUpsertInQuery(boolean enabled) {
    _upsertInQueryEnabled = enabled;
  }
}
