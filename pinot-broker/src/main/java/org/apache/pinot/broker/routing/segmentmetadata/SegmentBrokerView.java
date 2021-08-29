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
package org.apache.pinot.broker.routing.segmentmetadata;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.broker.routing.segmentmetadata.PartitionInfo.INVALID_PARTITION_INFO;


public class SegmentBrokerView {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentBrokerView.class);
  public static final long MIN_START_TIME = 0;
  public static final long MAX_END_TIME = Long.MAX_VALUE;
  private static final Interval DEFAULT_INTERVAL = new Interval(MIN_START_TIME, MAX_END_TIME);
  private Interval _segmentInterval;
  private PartitionInfo _partitionInfo;
  private long _totalDocs;
  private final String _segmentName;

  public SegmentBrokerView(String segmentName) {
    _segmentName = segmentName;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public Interval getInterval() {
    return _segmentInterval;
  }

  public void setInterval(Interval segmentInterval) {
    this._segmentInterval = segmentInterval;
  }

  public PartitionInfo getPartitionInfo() {
    return _partitionInfo;
  }

  public void setPartitionInfo(PartitionInfo partitionInfo) {
    this._partitionInfo = partitionInfo;
  }

  public long getTotalDocs() {
    return _totalDocs;
  }

  public void setTotalDocs(long totalDocs) {
    this._totalDocs = totalDocs;
  }

  // We consider segmentName as primary key and all other info irrelevant
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentBrokerView that = (SegmentBrokerView) o;
    return Objects.equals(_segmentName, that._segmentName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_segmentName);
  }

  private static long extractTotalDocsFromSegmentZKMetaZNRecord(@Nullable ZNRecord znRecord, String segment,
      String tableNameWithType) {
    if (znRecord == null) {
      LOGGER.warn("Failed to find segment ZK metadata for segment: {}, table: {}", segment, tableNameWithType);
      return -1;
    }
    return znRecord.getLongField(CommonConstants.Segment.TOTAL_DOCS, -1);
  }

  public static Interval extractIntervalFromSegmentZKMetaZNRecord(@Nullable ZNRecord znRecord, String segment,
      String tableNameWithType) {
    // Segments without metadata or with invalid time interval will be set with [min_start, max_end] and will not be
    // pruned
    if (znRecord == null) {
      LOGGER.warn("Failed to find segment ZK metadata for segment: {}, table: {}", segment, tableNameWithType);
      return DEFAULT_INTERVAL;
    }

    long startTime = znRecord.getLongField(CommonConstants.Segment.START_TIME, -1);
    long endTime = znRecord.getLongField(CommonConstants.Segment.END_TIME, -1);
    if (startTime < 0 || endTime < 0 || startTime > endTime) {
      LOGGER.warn("Failed to find valid time interval for segment: {}, table: {}", segment, tableNameWithType);
      return DEFAULT_INTERVAL;
    }

    TimeUnit timeUnit = znRecord.getEnumField(CommonConstants.Segment.TIME_UNIT, TimeUnit.class, TimeUnit.DAYS);
    return new Interval(timeUnit.toMillis(startTime), timeUnit.toMillis(endTime));
  }

  /**
   * NOTE: Returns {@code null} when the ZNRecord is missing (could be transient Helix issue). Returns
   *       INVALID_PARTITION_INFO when the segment does not have valid partition metadata in its ZK metadata,
   *       in which case we won't retry later.
   */
  @Nullable
  public static PartitionInfo extractPartitionInfoFromSegmentZKMetadataZNRecord(@Nullable ZNRecord znRecord,
      String partitionColumn, String segment, String tableNameWithType) {
    if (znRecord == null) {
      LOGGER.warn("Failed to find segment ZK metadata for segment: {}, table: {}", segment, tableNameWithType);
      return null;
    }

    String partitionMetadataJson = znRecord.getSimpleField(CommonConstants.Segment.PARTITION_METADATA);
    if (partitionMetadataJson == null) {
      LOGGER.warn("Failed to find segment partition metadata for segment: {}, table: {}", segment, tableNameWithType);
      return INVALID_PARTITION_INFO;
    }

    SegmentPartitionMetadata segmentPartitionMetadata;
    try {
      segmentPartitionMetadata = SegmentPartitionMetadata.fromJsonString(partitionMetadataJson);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while extracting segment partition metadata for segment: {}, table: {}", segment,
          tableNameWithType, e);
      return INVALID_PARTITION_INFO;
    }

    ColumnPartitionMetadata columnPartitionMetadata =
        segmentPartitionMetadata.getColumnPartitionMap().get(partitionColumn);
    if (columnPartitionMetadata == null) {
      LOGGER.warn("Failed to find column partition metadata for column: {}, segment: {}, table: {}", partitionColumn,
          segment, tableNameWithType);
      return INVALID_PARTITION_INFO;
    }

    return new PartitionInfo(PartitionFunctionFactory
        .getPartitionFunction(columnPartitionMetadata.getFunctionName(), columnPartitionMetadata.getNumPartitions()),
        columnPartitionMetadata.getPartitions());
  }

  public static Set<SegmentBrokerView> extractSegmentMetadata(TableConfig tableConfig, Set<String> onlineSegments,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    String tableNameWithType = tableConfig.getTableName();
    Set<String> partitionColumns = PartitionInfo.getPartitionColumnFromConfig(tableConfig);
    String segmentZKMetadataPathPrefix =
        ZKMetadataProvider.constructPropertyStorePathForResource(tableNameWithType) + "/";
    int numSegments = onlineSegments.size();
    List<String> segments = new ArrayList<>(numSegments);
    List<String> segmentZKMetadataPaths = new ArrayList<>(numSegments);
    Set<SegmentBrokerView> retVal = new HashSet<>(onlineSegments.size());
    for (String segment : onlineSegments) {
      segments.add(segment);
      segmentZKMetadataPaths.add(segmentZKMetadataPathPrefix + segment);
    }
    List<ZNRecord> znRecords = propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT);
    for (int i = 0; i < numSegments; i++) {
      String segment = segments.get(i);
      retVal.add(extractOneSegmentMetadata(segment, tableConfig, znRecords.get(i), partitionColumns));
    }
    return retVal;
  }

  public static SegmentBrokerView extractSegmentMetadata(String segmentName, TableConfig tableConfig,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    List<String> segmentZKMetadataPaths = new ArrayList<>(1);
    segmentZKMetadataPaths
        .add(ZKMetadataProvider.constructPropertyStorePathForResource(tableConfig.getTableName()) + "/" + segmentName);
    ZNRecord record = propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT).get(0);
    return extractOneSegmentMetadata(segmentName, tableConfig, record);
  }

  public static SegmentBrokerView extractOneSegmentMetadata(String segmentName, TableConfig tableConfig,
      ZNRecord znRecord) {
    Set<String> partitionColumns = PartitionInfo.getPartitionColumnFromConfig(tableConfig);
    return extractOneSegmentMetadata(segmentName, tableConfig, znRecord, partitionColumns);
  }

  private static SegmentBrokerView extractOneSegmentMetadata(String segmentName, TableConfig tableConfig,
      ZNRecord znRecord, Set<String> partitionColumns) {
    SegmentBrokerView segmentBrokerView = new SegmentBrokerView(segmentName);
    if (partitionColumns != null && partitionColumns.size() == 1) {
      PartitionInfo partitionInfo = SegmentBrokerView
          .extractPartitionInfoFromSegmentZKMetadataZNRecord(znRecord, partitionColumns.iterator().next(), segmentName,
              tableConfig.getTableName());
      segmentBrokerView.setPartitionInfo(partitionInfo);
    }
    Interval interval =
        SegmentBrokerView.extractIntervalFromSegmentZKMetaZNRecord(znRecord, segmentName, tableConfig.getTableName());
    segmentBrokerView.setInterval(interval);
    long totalDocs = extractTotalDocsFromSegmentZKMetaZNRecord(znRecord, segmentName, tableConfig.getTableName());
    segmentBrokerView.setTotalDocs(totalDocs);
    return segmentBrokerView;
  }
}
