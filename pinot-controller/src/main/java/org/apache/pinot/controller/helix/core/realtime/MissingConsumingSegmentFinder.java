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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.helix.core.PinotTableIdealStateBuilder;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * For a given table, this class finds out if there is any partition group for which there's no consuming segment in
 * ideal state. If so, it emits three metrics:
 *   - Total number of partitions with missing consuming segments including
 *   - Number of newly added partitions for which there's no consuming segment (there's no completed segment either)
 *   - Maximum duration (in minutes) that a partition hasn't had a consuming segment
 */
public class MissingConsumingSegmentFinder {
  private static final Logger LOGGER = LoggerFactory.getLogger(MissingConsumingSegmentFinder.class);

  private final String _realtimeTableName;
  private final SegmentMetadataFetcher _segmentMetadataFetcher;
  private final Map<Integer, StreamPartitionMsgOffset> _partitionGroupIdToLargestStreamOffsetMap;
  private final StreamPartitionMsgOffsetFactory _streamPartitionMsgOffsetFactory;

  private ControllerMetrics _controllerMetrics;

  public MissingConsumingSegmentFinder(String realtimeTableName, ZkHelixPropertyStore<ZNRecord> propertyStore,
      ControllerMetrics controllerMetrics, StreamConfig streamConfig) {
    _realtimeTableName = realtimeTableName;
    _controllerMetrics = controllerMetrics;
    _segmentMetadataFetcher = new SegmentMetadataFetcher(propertyStore, controllerMetrics);
    _streamPartitionMsgOffsetFactory =
        StreamConsumerFactoryProvider.create(streamConfig).createStreamMsgOffsetFactory();

    // create partition group id to largest stream offset map
    _partitionGroupIdToLargestStreamOffsetMap = new HashMap<>();
    streamConfig.setOffsetCriteria(OffsetCriteria.LARGEST_OFFSET_CRITERIA);
    try {
      PinotTableIdealStateBuilder.getPartitionGroupMetadataList(streamConfig, Collections.emptyList())
          .forEach(metadata -> {
            _partitionGroupIdToLargestStreamOffsetMap.put(metadata.getPartitionGroupId(), metadata.getStartOffset());
          });
    } catch (Exception e) {
      LOGGER.warn("Problem encountered in fetching stream metadata for topic: {} of table: {}. "
              + "Continue finding missing consuming segment only with ideal state information.",
          streamConfig.getTopicName(), streamConfig.getTableNameWithType());
    }
  }

  @VisibleForTesting
  MissingConsumingSegmentFinder(String realtimeTableName, SegmentMetadataFetcher segmentMetadataFetcher,
      Map<Integer, StreamPartitionMsgOffset> partitionGroupIdToLargestStreamOffsetMap,
      StreamPartitionMsgOffsetFactory streamPartitionMsgOffsetFactory) {
    _realtimeTableName = realtimeTableName;
    _segmentMetadataFetcher = segmentMetadataFetcher;
    _partitionGroupIdToLargestStreamOffsetMap = partitionGroupIdToLargestStreamOffsetMap;
    _streamPartitionMsgOffsetFactory = streamPartitionMsgOffsetFactory;
  }

  public void findAndEmitMetrics(IdealState idealState) {
    MissingSegmentInfo info = findMissingSegments(idealState.getRecord().getMapFields(), Instant.now());
    _controllerMetrics.setValueOfTableGauge(_realtimeTableName, ControllerGauge.MISSING_CONSUMING_SEGMENT_TOTAL_COUNT,
        info._totalCount);
    _controllerMetrics
        .setValueOfTableGauge(_realtimeTableName, ControllerGauge.MISSING_CONSUMING_SEGMENT_NEW_PARTITION_COUNT,
            info._newPartitionGroupCount);
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
      if (llcSegmentName != null) { // Skip the uploaded realtime segments that don't conform to llc naming
        if (instanceToStatusMap.containsValue(SegmentStateModel.CONSUMING)) {
          updateMap(partitionGroupIdToLatestConsumingSegmentMap, llcSegmentName);
        } else if (instanceToStatusMap.containsValue(SegmentStateModel.ONLINE)) {
          updateMap(partitionGroupIdToLatestCompletedSegmentMap, llcSegmentName);
        }
      }
    });

    MissingSegmentInfo missingSegmentInfo = new MissingSegmentInfo();
    if (!_partitionGroupIdToLargestStreamOffsetMap.isEmpty()) {
      _partitionGroupIdToLargestStreamOffsetMap.forEach((partitionGroupId, largestStreamOffset) -> {
        if (!partitionGroupIdToLatestConsumingSegmentMap.containsKey(partitionGroupId)) {
          LLCSegmentName latestCompletedSegment = partitionGroupIdToLatestCompletedSegmentMap.get(partitionGroupId);
          if (latestCompletedSegment == null) {
            // There's no consuming or completed segment for this partition group. Possibilities:
            //   1) it's a new partition group that has not yet been detected
            //   2) the first consuming segment has been deleted from ideal state manually
            missingSegmentInfo._newPartitionGroupCount++;
            missingSegmentInfo._totalCount++;
          } else {
            // Completed segment is available, but there's no consuming segment.
            // Note that there is no problem in case the partition group has reached its end of life.
            SegmentZKMetadata segmentZKMetadata = _segmentMetadataFetcher
                .fetchSegmentZkMetadata(_realtimeTableName, latestCompletedSegment.getSegmentName());
            StreamPartitionMsgOffset completedSegmentEndOffset =
                _streamPartitionMsgOffsetFactory.create(segmentZKMetadata.getEndOffset());
            if (completedSegmentEndOffset.compareTo(largestStreamOffset) < 0) {
              // there are unconsumed messages available on the stream
              missingSegmentInfo._totalCount++;
              updateMaxDurationInfo(missingSegmentInfo, partitionGroupId, segmentZKMetadata.getCreationTime(), now);
            }
          }
        }
      });
    } else {
      partitionGroupIdToLatestCompletedSegmentMap.forEach((partitionGroupId, latestCompletedSegment) -> {
        if (!partitionGroupIdToLatestConsumingSegmentMap.containsKey(partitionGroupId)) {
          missingSegmentInfo._totalCount++;
          long segmentCompletionTimeMillis = _segmentMetadataFetcher
              .fetchSegmentCompletionTime(_realtimeTableName, latestCompletedSegment.getSegmentName());
          updateMaxDurationInfo(missingSegmentInfo, partitionGroupId, segmentCompletionTimeMillis, now);
        }
      });
    }
    return missingSegmentInfo;
  }

  private void updateMaxDurationInfo(MissingSegmentInfo missingSegmentInfo, Integer partitionGroupId,
      long segmentCompletionTimeMillis, Instant now) {
    long duration = Duration.between(Instant.ofEpochMilli(segmentCompletionTimeMillis), now).toMinutes();
    if (duration > missingSegmentInfo._maxDurationInMinutes) {
      missingSegmentInfo._maxDurationInMinutes = duration;
    }
    LOGGER.warn("PartitionGroupId {} hasn't had a consuming segment for {} minutes!", partitionGroupId, duration);
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

  @VisibleForTesting
  static class MissingSegmentInfo {
    long _totalCount;
    long _newPartitionGroupCount;
    long _maxDurationInMinutes;
  }

  static class SegmentMetadataFetcher {
    private ZkHelixPropertyStore<ZNRecord> _propertyStore;
    private ControllerMetrics _controllerMetrics;

    public SegmentMetadataFetcher(ZkHelixPropertyStore<ZNRecord> propertyStore, ControllerMetrics controllerMetrics) {
      _propertyStore = propertyStore;
      _controllerMetrics = controllerMetrics;
    }

    public SegmentZKMetadata fetchSegmentZkMetadata(String tableName, String segmentName) {
      return fetchSegmentZkMetadata(tableName, segmentName, null);
    }

    public long fetchSegmentCompletionTime(String tableName, String segmentName) {
      Stat stat = new Stat();
      fetchSegmentZkMetadata(tableName, segmentName, stat);
      return stat.getMtime();
    }

    private SegmentZKMetadata fetchSegmentZkMetadata(String tableName, String segmentName, Stat stat) {
      try {
        ZNRecord znRecord = _propertyStore
            .get(ZKMetadataProvider.constructPropertyStorePathForSegment(tableName, segmentName), stat,
                AccessOption.PERSISTENT);
        Preconditions.checkState(znRecord != null, "Failed to find segment ZK metadata for segment: %s of table: %s",
            segmentName, tableName);
        return new SegmentZKMetadata(znRecord);
      } catch (Exception e) {
        _controllerMetrics.addMeteredTableValue(tableName, ControllerMeter.LLC_ZOOKEEPER_FETCH_FAILURES, 1L);
        throw e;
      }
    }
  }
}
