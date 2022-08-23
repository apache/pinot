package org.apache.pinot.server.starter.helix;

import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.realtime.LLRealtimeSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class IngestionBasedConsumptionStatusChecker {
  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());

  // constructor parameters
  protected final InstanceDataManager _instanceDataManager;
  protected final Set<String> _consumingSegments;

  // helper variable
  private final Set<String> _caughtUpSegments = new HashSet<>();

  public IngestionBasedConsumptionStatusChecker(InstanceDataManager instanceDataManager,
      Set<String> consumingSegments) {
    _instanceDataManager = instanceDataManager;
    _consumingSegments = consumingSegments;
  }

  public int getNumConsumingSegmentsNotReachedIngestionCriteria() {
    for (String segName : _consumingSegments) {
      if (_caughtUpSegments.contains(segName)) {
        continue;
      }
      TableDataManager tableDataManager = getTableDataManager(segName);
      if (tableDataManager == null) {
        LOGGER.info("TableDataManager is not yet setup for segment {}. Will check consumption status later", segName);
        continue;
      }
      SegmentDataManager segmentDataManager = null;
      try {
        segmentDataManager = tableDataManager.acquireSegment(segName);
        if (segmentDataManager == null) {
          LOGGER.info("SegmentDataManager is not yet setup for segment {}. Will check consumption status later",
              segName);
          continue;
        }
        if (!(segmentDataManager instanceof LLRealtimeSegmentDataManager)) {
          // There's a possibility that a consuming segment has converted to a committed segment. If that's the case,
          // segment data manager will not be of type LLRealtime.
          LOGGER.info("Segment {} is already committed and is considered caught up.", segName);
          _caughtUpSegments.add(segName);
          continue;
        }

        if (isSegmentCaughtUp(segName, segmentDataManager)) {
          _caughtUpSegments.add(segName);
        }
      } finally {
        if (segmentDataManager != null) {
          tableDataManager.releaseSegment(segmentDataManager);
        }
      }
    }
    return _consumingSegments.size() - _caughtUpSegments.size();
  }

  protected abstract boolean isSegmentCaughtUp(String segmentName, SegmentDataManager segmentDataManager);

  private TableDataManager getTableDataManager(String segmentName) {
    LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
    String tableName = llcSegmentName.getTableName();
    String tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(tableName);
    return _instanceDataManager.getTableDataManager(tableNameWithType);
  }
}
