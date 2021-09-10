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

package org.apache.pinot.server.starter.helix;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.realtime.LLRealtimeSegmentDataManager;
import org.apache.pinot.segment.local.data.manager.SegmentDataManager;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class is used at startup time to have a more accurate estimate of the catchup period in which no query execution
 * happens and consumers try to catch up to the latest messages available in streams.
 * To achieve this, every time status check is called, {@link #haveAllConsumingSegmentsReachedStreamLatestOffset},
 * list of consuming segments is gathered and then for each segment, we check if segment's latest ingested offset has
 * reached the latest stream offset. To prevent chasing a moving target, once the latest stream offset is fetched, it
 * will not be fetched again and subsequent status check calls compare latest ingested offset with the already fetched
 * stream offset.
 */
public class OffsetBasedConsumptionStatusChecker {
  private static final Logger LOGGER = LoggerFactory.getLogger(OffsetBasedConsumptionStatusChecker.class);
  static final long MAX_WAIT_TIME_MS = 5000; // for fetching latest stream offset

  private final InstanceDataManager _instanceDataManager;
  private Supplier<Set<String>> _consumingSegmentFinder;

  private Set<String> _caughtUpSegments = new HashSet<>();
  private Map<String, StreamPartitionMsgOffset> _segmentNameToLatestStreamOffset = new HashMap<>();

  public OffsetBasedConsumptionStatusChecker(InstanceDataManager instanceDataManager, HelixAdmin helixAdmin,
      String helixClusterName, String instanceId) {
    this(instanceDataManager, () -> findConsumingSegments(helixAdmin, helixClusterName, instanceId));
  }

  @VisibleForTesting
  OffsetBasedConsumptionStatusChecker(InstanceDataManager instanceDataManager,
      Supplier<Set<String>> consumingSegmentFinder) {
    _instanceDataManager = instanceDataManager;
    _consumingSegmentFinder = consumingSegmentFinder;
  }

  public boolean haveAllConsumingSegmentsReachedStreamLatestOffset() {
    boolean allSegsReachedLatest = true;
    Set<String> consumingSegmentNames = _consumingSegmentFinder.get();
    for (String segName : consumingSegmentNames) {
      if (_caughtUpSegments.contains(segName)) {
        continue;
      }
      TableDataManager tableDataManager = getTableDataManager(segName);
      if (tableDataManager == null) {
        LOGGER.info("TableDataManager is not yet setup for segment {}. Will check consumption status later", segName);
        return false;
      }
      SegmentDataManager segmentDataManager = tableDataManager.acquireSegment(segName);
      if (segmentDataManager == null) {
        LOGGER.info("SegmentDataManager is not yet setup for segment {}. Will check consumption status later", segName);
        return false;
      }
      if (!(segmentDataManager instanceof LLRealtimeSegmentDataManager)) {
        // There's a small chance that after getting the list of consuming segment names at the beginning of this method
        // up to this point, a consuming segment gets converted to a committed segment. In that case status check is
        // returned as false and in the next round the new consuming segment will be used for fetching offsets.
        LOGGER
            .info("Segment {} is already committed. Will check consumption status later on the next segment", segName);
        releaseSegment(tableDataManager, segmentDataManager);
        return false;
      }
      LLRealtimeSegmentDataManager rtSegmentDataManager = (LLRealtimeSegmentDataManager) segmentDataManager;
      StreamPartitionMsgOffset latestIngestedOffset = rtSegmentDataManager.getCurrentOffset();
      StreamPartitionMsgOffset latestStreamOffset = _segmentNameToLatestStreamOffset.containsKey(segName)
          ? _segmentNameToLatestStreamOffset.get(segName)
          : rtSegmentDataManager.fetchLatestStreamOffset(MAX_WAIT_TIME_MS);
      releaseSegment(tableDataManager, segmentDataManager);
      if (latestStreamOffset == null || latestIngestedOffset == null) {
        LOGGER.info("Null offset found for segment {} - latest stream offset: {}, latest ingested offset: {}. "
            + "Will check consumption status later", segName, latestStreamOffset, latestIngestedOffset);
        return false;
      }
      if (latestIngestedOffset.compareTo(latestStreamOffset) < 0) {
        LOGGER.info("Latest ingested offset {} in segment {} is smaller than stream latest available offset {} ",
            latestIngestedOffset, segName, latestStreamOffset);
        _segmentNameToLatestStreamOffset.put(segName, latestStreamOffset);
        allSegsReachedLatest = false;
        continue;
      }
      LOGGER.info("Segment {} with latest ingested offset {} has caught up to the latest stream offset {}", segName,
            latestIngestedOffset, latestStreamOffset);
      _caughtUpSegments.add(segName);
    }
    return allSegsReachedLatest;
  }

  void releaseSegment(TableDataManager tableDataManager, SegmentDataManager segmentDataManager) {
    try {
      tableDataManager.releaseSegment(segmentDataManager);
    } catch (Exception e) {
      LOGGER.error("Cannot release segment data manager for segment " + segmentDataManager.getSegmentName(), e);
    }
  }

  private TableDataManager getTableDataManager(String segmentName) {
    LLCSegmentName llcSegmentName = new LLCSegmentName(segmentName);
    String tableName = llcSegmentName.getTableName();
    String tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(tableName);
    return _instanceDataManager.getTableDataManager(tableNameWithType);
  }

  private static Set<String> findConsumingSegments(HelixAdmin helixAdmin, String helixClusterName, String instanceId) {
    Set<String> consumingSegments = new HashSet<>();
    for (String resourceName : helixAdmin.getResourcesInCluster(helixClusterName)) {
      if (TableNameBuilder.isRealtimeTableResource(resourceName)) {
        IdealState idealState = helixAdmin.getResourceIdealState(helixClusterName, resourceName);
        if (idealState.isEnabled()) {
          for (String segName : idealState.getPartitionSet()) {
            if (CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING
                .equals(idealState.getInstanceStateMap(segName).get(instanceId))) {
              consumingSegments.add(segName);
            }
          }
        }
      }
    }
    return consumingSegments;
  }

  @VisibleForTesting
  void setConsumingSegmentFinder(Supplier<Set<String>> consumingSegmentFinder) {
    _consumingSegmentFinder = consumingSegmentFinder;
  }
}
