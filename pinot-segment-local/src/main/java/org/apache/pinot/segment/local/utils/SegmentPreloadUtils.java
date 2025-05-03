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
package org.apache.pinot.segment.local.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiPredicate;
import javax.annotation.Nullable;
import org.apache.helix.HelixManager;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.SegmentUtils;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.segment.local.segment.index.column.DefaultNullValueVirtualColumnProvider;
import org.apache.pinot.segment.local.segment.index.datasource.ImmutableDataSource;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProvider;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentPreloadUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPreloadUtils.class);

  private SegmentPreloadUtils() {
  }

  public static void preloadSegments(TableDataManager tableDataManager, int partitionId,
      IndexLoadingConfig indexLoadingConfig, HelixManager helixManager, ExecutorService segmentPreloadExecutor,
      @Nullable BiPredicate<String, SegmentZKMetadata> segmentSelector)
      throws Exception {
    String tableNameWithType = tableDataManager.getTableName();
    LOGGER.info("Preload segments from partition: {} of table: {} for fast metadata recovery", partitionId,
        tableNameWithType);
    Map<String, Map<String, String>> segmentAssignment = getSegmentAssignment(tableNameWithType, helixManager);
    Map<String, SegmentZKMetadata> segmentMetadataMap = getSegmentsZKMetadata(tableNameWithType, helixManager);
    List<String> preloadedSegments =
        doPreloadSegments(tableDataManager, partitionId, indexLoadingConfig, segmentAssignment, segmentMetadataMap,
            segmentPreloadExecutor, segmentSelector);
    LOGGER.info("Preloaded {} segments from partition: {} of table: {} for fast metadata recovery",
        preloadedSegments.size(), partitionId, tableNameWithType);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Preloaded segments: {}", preloadedSegments);
    }
  }

  @VisibleForTesting
  static List<String> doPreloadSegments(TableDataManager tableDataManager, int partitionId,
      IndexLoadingConfig indexLoadingConfig, Map<String, Map<String, String>> segmentAssignment,
      Map<String, SegmentZKMetadata> segmentMetadataMap, ExecutorService segmentPreloadExecutor,
      @Nullable BiPredicate<String, SegmentZKMetadata> segmentSelector)
      throws ExecutionException, InterruptedException {
    String tableNameWithType = tableDataManager.getTableName();
    String instanceId = getInstanceId(tableDataManager);
    List<String> preloadedSegments = new ArrayList<>();
    List<Future<?>> futures = new ArrayList<>();
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      String segmentName = entry.getKey();
      Map<String, String> instanceStateMap = entry.getValue();
      if (!isSegmentOnlineOnRequestedInstance(segmentName, instanceId, instanceStateMap)) {
        continue;
      }
      SegmentZKMetadata segmentZKMetadata = segmentMetadataMap.get(segmentName);
      if (!isSegmentFromRequestedPartition(segmentName, tableNameWithType, partitionId, segmentZKMetadata)) {
        continue;
      }
      if (segmentSelector != null && !segmentSelector.test(segmentName, segmentZKMetadata)) {
        continue;
      }
      futures.add(segmentPreloadExecutor.submit(
          () -> preloadSegment(segmentName, tableDataManager, partitionId, indexLoadingConfig, segmentZKMetadata)));
      preloadedSegments.add(segmentName);
    }
    waitForSegmentsPreloaded(futures);
    return preloadedSegments;
  }

  private static void preloadSegment(String segmentName, TableDataManager tableDataManager, int partitionId,
      IndexLoadingConfig indexLoadingConfig, SegmentZKMetadata segmentZKMetadata) {
    String tableNameWithType = tableDataManager.getTableName();
    try {
      LOGGER.info("Preload segment: {} from partition: {} of table: {}", segmentName, partitionId, tableNameWithType);
      // This method checks segment crc, and the segment is not loaded if the crc has changed.
      tableDataManager.tryLoadExistingSegment(segmentZKMetadata, indexLoadingConfig);
      LOGGER.info("Preloaded segment: {} from partition: {} of table: {}", segmentName, partitionId, tableNameWithType);
    } catch (Exception e) {
      LOGGER.warn("Failed to preload segment: {} from partition: {} of table: {}, skipping", segmentName, partitionId,
          tableNameWithType, e);
    }
  }

  private static void waitForSegmentsPreloaded(List<Future<?>> futures)
      throws ExecutionException, InterruptedException {
    try {
      for (Future<?> f : futures) {
        f.get();
      }
    } finally {
      for (Future<?> f : futures) {
        if (!f.isDone()) {
          f.cancel(true);
        }
      }
    }
  }

  public static boolean hasValidDocIdsSnapshot(TableDataManager tableDataManager, TableConfig tableConfig,
      String segmentName, String segmentTier) {
    try {
      File indexDir = tableDataManager.getSegmentDataDir(segmentName, segmentTier, tableConfig);
      File snapshotFile =
          new File(SegmentDirectoryPaths.findSegmentDirectory(indexDir), V1Constants.VALID_DOC_IDS_SNAPSHOT_FILE_NAME);
      return snapshotFile.exists();
    } catch (Exception e) {
      return false;
    }
  }

  private static boolean isSegmentOnlineOnRequestedInstance(String segmentName, String instanceId,
      Map<String, String> instanceStateMap) {
    String state = instanceStateMap.get(instanceId);
    if (CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE.equals(state)) {
      return true;
    }
    if (state == null) {
      LOGGER.debug("Skip segment: {} as it's not assigned to instance: {}", segmentName, instanceId);
    } else {
      LOGGER.info("Skip segment: {} as its ideal state: {} is not ONLINE for instance: {}", segmentName, state,
          instanceId);
    }
    return false;
  }

  private static boolean isSegmentFromRequestedPartition(String segmentName, String tableNameWithType,
      int requestedPartitionId, SegmentZKMetadata segmentZKMetadata) {
    Preconditions.checkState(segmentZKMetadata != null, "Failed to find ZK metadata for segment: %s, table: %s",
        segmentName, tableNameWithType);
    Integer partitionId = SegmentUtils.getRealtimeSegmentPartitionId(segmentName, segmentZKMetadata, null);
    Preconditions.checkNotNull(partitionId,
        String.format("Failed to get partition id for segment: %s from table: %s", segmentName, tableNameWithType));
    if (partitionId == requestedPartitionId) {
      return true;
    }
    LOGGER.debug("Skip segment: {} as its partition: {} is different from the requested partition: {}", segmentName,
        partitionId, requestedPartitionId);
    return false;
  }

  private static String getInstanceId(TableDataManager tableDataManager) {
    return tableDataManager.getInstanceDataManagerConfig().getInstanceId();
  }

  private static Map<String, Map<String, String>> getSegmentAssignment(String tableNameWithType,
      HelixManager helixManager) {
    IdealState idealState = HelixHelper.getTableIdealState(helixManager, tableNameWithType);
    Preconditions.checkState(idealState != null, "Failed to find ideal state for table: %s", tableNameWithType);
    return idealState.getRecord().getMapFields();
  }

  private static Map<String, SegmentZKMetadata> getSegmentsZKMetadata(String tableNameWithType,
      HelixManager helixManager) {
    Map<String, SegmentZKMetadata> segmentMetadataMap = new HashMap<>();
    ZKMetadataProvider.getSegmentsZKMetadata(helixManager.getHelixPropertyStore(), tableNameWithType)
        .forEach(m -> segmentMetadataMap.put(m.getSegmentName(), m));
    return segmentMetadataMap;
  }

  public static DataSource getVirtualDataSource(IndexLoadingConfig indexLoadingConfig, String column,
                                                int totalDocCount) {
    // First check is the schema is already updated with the virtual column
    Schema schema = indexLoadingConfig.getSchema();
    assert schema != null : "Schema should not be null";
    if (!schema.hasColumn(column)) {
      // Get the latest schema from the table data manager
      assert indexLoadingConfig.getTableConfig() != null;
      schema = ZKMetadataProvider.getTableSchema(indexLoadingConfig.getPropertyStore(),
              indexLoadingConfig.getTableConfig().getTableName());
      indexLoadingConfig.setSchema(schema);
    }
    assert schema != null;
    FieldSpec fieldSpec = schema.getFieldSpecFor(column);
    fieldSpec.setVirtualColumnProvider(DefaultNullValueVirtualColumnProvider.class.getName());
    VirtualColumnContext virtualColumnContext = new VirtualColumnContext(fieldSpec, totalDocCount);
    VirtualColumnProvider virtualColumnProvider = VirtualColumnProviderFactory.buildProvider(virtualColumnContext);
    return new ImmutableDataSource(virtualColumnProvider.buildMetadata(virtualColumnContext),
            virtualColumnProvider.buildColumnIndexContainer(virtualColumnContext));
  }
}
