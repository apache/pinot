package org.apache.pinot.core.upsert;

import java.util.Map;
import org.apache.pinot.core.realtime.impl.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.spi.data.readers.PrimaryKey;


public final class UpsertProcessorUtil {
  private UpsertProcessorUtil() {
  }

  public static void handleUpsert(PrimaryKey primaryKey, long timestamp, String segmentName, int docId, int partitionId,
      Map<PrimaryKey, RecordLocation> primaryKeyIndex, ThreadSafeMutableRoaringBitmap validDocIndex,
      UpsertMetadataTableManager upsertMetadataTableManager) {
    RecordLocation location = new RecordLocation(segmentName, docId, timestamp);
    // check local primary key index first
    if (primaryKeyIndex.containsKey(primaryKey)) {
      RecordLocation prevLocation = primaryKeyIndex.get(primaryKey);
      if (location.getTimestamp() >= prevLocation.getTimestamp()) {
        primaryKeyIndex.put(primaryKey, location);
        // update validDocIndex
        validDocIndex.remove(prevLocation.getDocId());
        validDocIndex.checkAndAdd(location.getDocId());
        System.out.println(String
            .format("upsert: replace old doc id %d with %d for key: %s, hash: %d", prevLocation.getDocId(),
                location.getDocId(), primaryKey, primaryKey.hashCode()));
      } else {
        System.out.println(
            String.format("upsert: ignore a late-arrived record: %s, hash: %d", primaryKey, primaryKey.hashCode()));
      }
    } else if (upsertMetadataTableManager.containsKey(partitionId, primaryKey)) {
      RecordLocation prevLocation = upsertMetadataTableManager.getRecordLocation(partitionId, primaryKey);
      if (location.getTimestamp() >= prevLocation.getTimestamp()) {
        upsertMetadataTableManager.removeRecordLocation(partitionId, primaryKey);
        primaryKeyIndex.put(primaryKey, location);

        // update validDocIndex
        upsertMetadataTableManager.getValidDocIndex(partitionId, prevLocation.getSegmentName())
            .remove(prevLocation.getDocId());
        validDocIndex.checkAndAdd(location.getDocId());
      }
    } else {
      primaryKeyIndex.put(primaryKey, location);
      validDocIndex.checkAndAdd(location.getDocId());
    }
  }
}
