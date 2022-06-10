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
package org.apache.pinot.controller.helix.core.realtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.zookeeper.data.Stat;


/**
 * For a given table, this class finds out if there is any partition group for which there's no consuming segment in
 * ideal state. If so, it emits two metrics:
 *   - Number of missing consuming segments
 *   - Maximum duration (in minutes) that a partition hasn't had a consuming segment
 */
public class MissingConsumingSegmentFinder {

  private String _realtimeTableName;
  private ControllerMetrics _controllerMetrics;
  private SegmentMetadataFetcher _segmentMetadataFetcher;

  public MissingConsumingSegmentFinder(String realtimeTableName, ZkHelixPropertyStore<ZNRecord> propertyStore,
      ControllerMetrics controllerMetrics) {
    _realtimeTableName = realtimeTableName;
    _controllerMetrics = controllerMetrics;
    _segmentMetadataFetcher = new SegmentMetadataFetcher(propertyStore, controllerMetrics);
  }

  @VisibleForTesting
  MissingConsumingSegmentFinder(String realtimeTableName, SegmentMetadataFetcher segmentMetadataFetcher) {
    _realtimeTableName = realtimeTableName;
    _segmentMetadataFetcher = segmentMetadataFetcher;
  }

  public void findAndEmitMetrics(IdealState idealState) {
    MissingSegmentInfo info = findMissingSegments(idealState.getRecord().getMapFields(), Instant.now());
    _controllerMetrics.setValueOfTableGauge(_realtimeTableName, ControllerGauge.MISSING_CONSUMING_SEGMENT_COUNT,
        info._count);
    _controllerMetrics
        .setValueOfTableGauge(_realtimeTableName, ControllerGauge.MISSING_CONSUMING_SEGMENT_MAX_DURATION_MINUTES,
            info._maxDurationInMinutes);
  }

  @VisibleForTesting
  MissingSegmentInfo findMissingSegments(Map<String, Map<String, String>> idealStateMap, Instant now) {
    // create the maps
    Map<Integer, LLCSegmentName> partitionGroupIdToLatestConsumingSegmentMap = new HashMap<>();
    Map<Integer, LLCSegmentName> partitionGroupIdToLatestCompletedSegmentMap = new HashMap<>();
    idealStateMap.forEach((segmentName, instanceToStatusMap) -> {
      LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
      if (llcSegmentName != null) { // TODO add comments for uploaded segments
        if (instanceToStatusMap.containsValue(SegmentStateModel.CONSUMING)) {
          updateMap(partitionGroupIdToLatestConsumingSegmentMap, llcSegmentName);
        } else if (instanceToStatusMap.containsValue(SegmentStateModel.ONLINE)) {
          updateMap(partitionGroupIdToLatestCompletedSegmentMap, llcSegmentName);
        }
      }
    });

    MissingSegmentInfo missingSegmentInfo = new MissingSegmentInfo();
    partitionGroupIdToLatestCompletedSegmentMap.forEach((partitionGroupId, latestCompletedSegment) -> {
      if (!partitionGroupIdToLatestConsumingSegmentMap.containsKey(partitionGroupId)) {
        missingSegmentInfo._count++;
        long segmentCompletionTimeMillis = getSegmentCompletionTimeFromZk(latestCompletedSegment.getSegmentName());
        long duration = Duration.between(Instant.ofEpochMilli(segmentCompletionTimeMillis), now).toMinutes();
        if (duration > missingSegmentInfo._maxDurationInMinutes) {
          missingSegmentInfo._maxDurationInMinutes = duration;
        }
      }
    });
    return missingSegmentInfo;
  }

  private void updateMap(Map<Integer, LLCSegmentName> partitionGroupIdToLatestSegmentMap,
      LLCSegmentName llcSegmentName) {
    int partitionGroupId = llcSegmentName.getPartitionGroupId();
    partitionGroupIdToLatestSegmentMap.compute(partitionGroupId, (pid, existingSegment) -> {
      if (existingSegment == null) {
        return llcSegmentName;
      } else {
        return existingSegment.getSequenceNumber() > llcSegmentName.getSequenceNumber() ? existingSegment
            : llcSegmentName;
      }
    });
  }

  private long getSegmentCompletionTimeFromZk(String segmentName) {
    return _segmentMetadataFetcher.fetchZNodeModificationTime(_realtimeTableName, segmentName);
  }

  @VisibleForTesting
  static class MissingSegmentInfo {
    long _count;
    long _maxDurationInMinutes;
  }

  static class SegmentMetadataFetcher {
    private ZkHelixPropertyStore<ZNRecord> _propertyStore;
    private ControllerMetrics _controllerMetrics;

    public SegmentMetadataFetcher(ZkHelixPropertyStore<ZNRecord> propertyStore, ControllerMetrics controllerMetrics) {
      _propertyStore = propertyStore;
      _controllerMetrics = controllerMetrics;
    }

    public long fetchZNodeModificationTime(String tableName, String segmentName) {
      try {
        Stat stat = new Stat();
        ZNRecord znRecord = _propertyStore
            .get(ZKMetadataProvider.constructPropertyStorePathForSegment(tableName, segmentName), stat,
                AccessOption.PERSISTENT);
        Preconditions.checkState(znRecord != null, "Failed to find segment ZK metadata for segment: %s of table: %s",
            segmentName, tableName);
        return stat.getMtime();
      } catch (Exception e) {
        _controllerMetrics.addMeteredTableValue(tableName, ControllerMeter.LLC_ZOOKEEPER_FETCH_FAILURES, 1L);
        throw e;
      }
    }
  }
}
