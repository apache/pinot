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
package org.apache.pinot.controller.helix.core.minion.generator;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.pinot.common.lineage.LineageEntry;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.controller.helix.core.minion.mergestrategy.MergeStrategyFactory;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.core.segment.processing.collector.CollectorFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentMergeRollupTaskGenerator implements PinotTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMergeRollupTaskGenerator.class);

  private static final int DEFAULT_MAX_NUM_SEGMENTS_PER_TASK = 20; // 20 segments
  private static final int DEFAULT_MAX_NUM_RECORDS_PER_SEGMENT = 5_000_000; // 5 million rows
  private static final int DEFAULT_MAX_NUM_TASKS = 200; // schedule at most 200 tasks at a time
  private static final String DEFAULT_BUFFER_TIME_PERIOD = "14d"; // 2 weeks

  private final ClusterInfoAccessor _clusterInfoAccessor;

  public SegmentMergeRollupTaskGenerator(ClusterInfoAccessor clusterInfoAccessor) {
    _clusterInfoAccessor = clusterInfoAccessor;
  }

  @Override
  public String getTaskType() {
    return MinionConstants.MergeRollupTask.TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();

    // Get the segments that are being converted so that we don't submit them again
    Map<String, Set<String>> scheduledSegmentsMap =
        TaskGeneratorUtils.getScheduledSegmentsMap(MinionConstants.MergeRollupTask.TASK_TYPE, _clusterInfoAccessor);

    // Generate tasks
    int numTasks = 0;
    for (TableConfig tableConfig : tableConfigs) {
      // Only generate tasks for OFFLINE tables
      String offlineTableName = tableConfig.getTableName();
      if (tableConfig.getTableType() != TableType.OFFLINE) {
        LOGGER.warn("Skip generating MergeRollupTask for non-OFFLINE table: {}", offlineTableName);
        continue;
      }

      TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
      Preconditions.checkNotNull(tableTaskConfig);
      Map<String, String> taskConfigs =
          tableTaskConfig.getConfigsForTaskType(MinionConstants.MergeRollupTask.TASK_TYPE);
      Preconditions.checkNotNull(taskConfigs, "Task config shouldn't be null for Table: {}", offlineTableName);

      int maxNumSegmentsPerTask =
          readIntConfigWithDefaultValue(taskConfigs, MinionConstants.MergeRollupTask.MAX_NUM_SEGMENTS_PER_TASK_KEY,
              DEFAULT_MAX_NUM_SEGMENTS_PER_TASK);

      int maxNumRecordsPerSegment =
          readIntConfigWithDefaultValue(taskConfigs, MinionConstants.MergeRollupTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY,
              DEFAULT_MAX_NUM_RECORDS_PER_SEGMENT);

      String bufferTimePeriod =
          taskConfigs.getOrDefault(MinionConstants.MergeRollupTask.BUFFER_TIME_PERIOD_KEY, DEFAULT_BUFFER_TIME_PERIOD);
      long bufferTimePeriodMs;
      try {
        bufferTimePeriodMs= TimeUtils.convertPeriodToMillis(bufferTimePeriod);
      } catch (IllegalArgumentException e) {
        LOGGER.error("Buffer time period ('{}') for table '{}' is not configured correctly.", bufferTimePeriod,
            offlineTableName, e);
        throw e;
      }

      List<OfflineSegmentZKMetadata> segmentsForOfflineTable =
          _clusterInfoAccessor.getOfflineSegmentsMetadata(offlineTableName);

      // Fetch the segment lineage for the table and compute the segments that should not be scheduled for merge
      // based on the segment lineage.
      SegmentLineage segmentLineageForTable = _clusterInfoAccessor.getSegmentLineage(offlineTableName);
      Set<String> segmentsNotToMerge = new HashSet<>();
      if (segmentLineageForTable != null) {
        for (String segmentLineageEntryId : segmentLineageForTable.getLineageEntryIds()) {
          LineageEntry lineageEntry = segmentLineageForTable.getLineageEntry(segmentLineageEntryId);
          // Segments shows up on "segmentFrom" field in the lineage entry should not be scheduled again.
          segmentsNotToMerge.addAll(lineageEntry.getSegmentsFrom());

          // Segments shows up on "segmentsTo" field in the lineage entry with "IN_PROGRESS" state cannot be merged.
          if (lineageEntry.getState() == LineageEntryState.IN_PROGRESS) {
            segmentsNotToMerge.addAll(lineageEntry.getSegmentsTo());
          }
        }
      }

      // Filter out the segments that cannot be merged
      List<SegmentZKMetadata> segmentsToMergeForTable = new ArrayList<>();
      Set<String> scheduledSegments = scheduledSegmentsMap.getOrDefault(offlineTableName, Collections.emptySet());
      for (OfflineSegmentZKMetadata offlineSegmentZKMetadata : segmentsForOfflineTable) {
        String segmentName = offlineSegmentZKMetadata.getSegmentName();

        // The segment should not be merged if it's already scheduled or in progress
        if (scheduledSegments.contains(segmentName) || segmentsNotToMerge.contains(segmentName)) {
          continue;
        }

        // The segments that are newer than the buffer time period should not be be merged
        if (System.currentTimeMillis() - offlineSegmentZKMetadata.getEndTime() < bufferTimePeriodMs) {
          continue;
        }
        segmentsToMergeForTable.add(offlineSegmentZKMetadata);
      }

      // Compute Merge Strategy
      List<List<SegmentZKMetadata>> segmentsToSchedule = MergeStrategyFactory.getMergeStrategy(taskConfigs)
          .generateMergeTaskCandidates(segmentsToMergeForTable, maxNumSegmentsPerTask);

      // Generate tasks
      for (List<SegmentZKMetadata> segments : segmentsToSchedule) {
        // TODO: wire the max num tasks to the cluster config to make it configurable.
        if (numTasks >= DEFAULT_MAX_NUM_TASKS) {
          break;
        }
        if (segments.size() >= 1) {
          Map<String, String> configs = new HashMap<>();
          configs.put(MinionConstants.TABLE_NAME_KEY, offlineTableName);
          configs.put(MinionConstants.SEGMENT_NAME_KEY,
              segments.stream().map(s -> s.getSegmentName()).collect(Collectors.joining(",")));
          configs.put(MinionConstants.DOWNLOAD_URL_KEY,
              segments.stream().map(s -> ((OfflineSegmentZKMetadata) s).getDownloadUrl())
                  .collect(Collectors.joining(",")));
          configs.put(MinionConstants.VIP_URL_KEY, _clusterInfoAccessor.getVipUrl());
          configs.put(MinionConstants.REPLACE_SEGMENTS_KEY, Boolean.TRUE.toString());
          configs.put(MinionConstants.MergeRollupTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY,
              Integer.toString(maxNumRecordsPerSegment));

          // TODO: support ROLLUP type
          configs.put(MinionConstants.MergeRollupTask.COLLECTOR_TYPE_KEY,
              CollectorFactory.CollectorType.CONCAT.toString());
          pinotTaskConfigs.add(new PinotTaskConfig(MinionConstants.MergeRollupTask.TASK_TYPE, configs));
          numTasks++;
        }
      }
    }
    return pinotTaskConfigs;
  }

  private int readIntConfigWithDefaultValue(Map<String, String> configs, String key, int defaultValue) {
    int intConfigValue;
    String intConfigStrValue = configs.get(key);
    if (intConfigStrValue != null) {
      try {
        intConfigValue = Integer.parseInt(intConfigStrValue);
      } catch (Exception e) {
        intConfigValue = defaultValue;
      }
    } else {
      intConfigValue = defaultValue;
    }
    return intConfigValue;
  }
}
