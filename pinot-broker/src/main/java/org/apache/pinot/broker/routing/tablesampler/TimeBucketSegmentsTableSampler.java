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
package org.apache.pinot.broker.routing.tablesampler;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.MapUtils;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.UploadedRealtimeSegmentName;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.sampler.TableSamplerConfig;
import org.apache.pinot.spi.utils.CommonConstants.Segment;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

/**
 * Selects up to N segments per time bucket, where the bucket size is configurable in days or hours.
 *
 * <p>Config:
 * <ul>
 *   <li>{@code properties.numSegmentsPerDay}: positive integer (required unless numSegmentsPerHour is set). Note:
 *   interpreted as "per bucket".</li>
 *   <li>{@code properties.numSegmentsPerHour}: positive integer (optional, interpreted as "per bucket").</li>
 *   <li>{@code properties.bucketDays}: positive integer (optional, default 1)</li>
 *   <li>{@code properties.bucketHours}: positive integer (optional, mutually exclusive with bucketDays)</li>
 * </ul>
 *
 * <p>Notes:
 * <ul>
 *   <li>Bucket boundaries are computed in UTC.</li>
 *   <li>If the table has a single partition column configured, selection is done per partition within each time
 *   bucket (one partition id per segment required).</li>
 *   <li>For OFFLINE tables, bucketing uses segment end time from ZK metadata.</li>
 *   <li>For REALTIME tables, bucketing tries to derive a timestamp from the segment name (LLC / uploaded realtime)
 *   to avoid ZK reads.</li>
 *   <li>Segments with unparsable/missing timestamps are skipped.</li>
 *   <li>Selection is deterministic: within each bucket, choose lexicographically first N segment names.</li>
 * </ul>
 */
public class TimeBucketSegmentsTableSampler implements TableSampler {
  public static final String TYPE = "timeBucket";

  public static final String PROP_NUM_SEGMENTS = "numSegmentsPerDay";
  public static final String PROP_NUM_SEGMENTS_PER_HOUR = "numSegmentsPerHour";
  public static final String PROP_BUCKET_DAYS = "bucketDays";
  public static final String PROP_BUCKET_HOURS = "bucketHours";

  private static final DateTimeFormatter REALTIME_SEGMENT_NAME_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmm'Z'");

  private String _tableNameWithType;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private int _numSegmentsPerBucket;
  private int _bucketDays;
  private int _bucketHours;
  private boolean _useHourBuckets;
  private boolean _usePartitionAware;
  private String _partitionColumn;
  private int _numPartitions;
  private boolean _isRealtimeTable;

  // Cache bucket->segments to avoid re-reading ZK metadata for unchanged segments.
  private static final int DEFAULT_PARTITION_KEY = 0;
  private final Map<Long, Map<Integer, NavigableSet<String>>> _bucketIdToPartitionSegments = new HashMap<>();
  private final Map<String, Long> _segmentToBucketId = new HashMap<>();
  private final Map<String, Integer> _segmentToPartitionId = new HashMap<>();

  @Override
  public void init(String tableNameWithType, TableConfig tableConfig, TableSamplerConfig samplerConfig,
      ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _tableNameWithType = tableNameWithType;
    _propertyStore = propertyStore;
    _isRealtimeTable = TableNameBuilder.isRealtimeTableResource(tableNameWithType);

    Map<String, String> props = samplerConfig.getProperties();
    if (MapUtils.isEmpty(props)
        || (!props.containsKey(PROP_NUM_SEGMENTS) && !props.containsKey(PROP_NUM_SEGMENTS_PER_HOUR))) {
      throw new IllegalArgumentException("Missing required property '" + PROP_NUM_SEGMENTS + "' or '"
          + PROP_NUM_SEGMENTS_PER_HOUR + "' for table sampler type '" + TYPE + "'");
    }

    boolean hasNumSegmentsPerDay = props.containsKey(PROP_NUM_SEGMENTS);
    boolean hasNumSegmentsPerHour = props.containsKey(PROP_NUM_SEGMENTS_PER_HOUR);
    if (hasNumSegmentsPerDay && hasNumSegmentsPerHour) {
      throw new IllegalArgumentException("'" + PROP_NUM_SEGMENTS + "' and '" + PROP_NUM_SEGMENTS_PER_HOUR
          + "' are mutually exclusive");
    }
    String numSegmentsStr =
        hasNumSegmentsPerHour ? props.get(PROP_NUM_SEGMENTS_PER_HOUR) : props.get(PROP_NUM_SEGMENTS);
    _numSegmentsPerBucket = Integer.parseInt(numSegmentsStr);
    if (_numSegmentsPerBucket <= 0) {
      throw new IllegalArgumentException("numSegments per bucket must be positive");
    }

    boolean hasBucketDays = props.containsKey(PROP_BUCKET_DAYS);
    boolean hasBucketHours = props.containsKey(PROP_BUCKET_HOURS);
    if (hasBucketDays && hasBucketHours) {
      throw new IllegalArgumentException("'" + PROP_BUCKET_DAYS + "' and '" + PROP_BUCKET_HOURS
          + "' are mutually exclusive");
    }

    if (hasNumSegmentsPerHour || hasBucketHours) {
      _bucketHours = Integer.parseInt(props.getOrDefault(PROP_BUCKET_HOURS, "1"));
      if (_bucketHours <= 0) {
        throw new IllegalArgumentException("'" + PROP_BUCKET_HOURS + "' must be positive");
      }
      _useHourBuckets = true;
    } else {
      String bucketDaysStr = props.getOrDefault(PROP_BUCKET_DAYS, "1");
      _bucketDays = Integer.parseInt(bucketDaysStr);
      if (_bucketDays <= 0) {
        throw new IllegalArgumentException("'" + PROP_BUCKET_DAYS + "' must be positive");
      }
    }

    SegmentPartitionConfig segmentPartitionConfig = tableConfig.getIndexingConfig() != null
        ? tableConfig.getIndexingConfig().getSegmentPartitionConfig() : null;
    if (segmentPartitionConfig != null && segmentPartitionConfig.getColumnPartitionMap() != null
        && !segmentPartitionConfig.getColumnPartitionMap().isEmpty()) {
      if (segmentPartitionConfig.getColumnPartitionMap().size() != 1) {
        throw new IllegalArgumentException("Time bucket sampler supports only a single partition column");
      }
      Map.Entry<String, ColumnPartitionConfig> entry =
          segmentPartitionConfig.getColumnPartitionMap().entrySet().iterator().next();
      _partitionColumn = entry.getKey();
      _numPartitions = entry.getValue().getNumPartitions();
      _usePartitionAware = true;
    } else {
      _usePartitionAware = false;
    }
  }

  @Override
  public Set<String> selectSegments(Set<String> onlineSegments) {
    if (onlineSegments.isEmpty()) {
      return Collections.emptySet();
    }

    // Remove segments that are no longer online.
    Iterator<Map.Entry<String, Long>> iterator = _segmentToBucketId.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, Long> entry = iterator.next();
      String segmentName = entry.getKey();
      if (!onlineSegments.contains(segmentName)) {
        Long bucketId = entry.getValue();
        Integer partitionId = _segmentToPartitionId.get(segmentName);
        if (partitionId != null) {
          Map<Integer, NavigableSet<String>> partitionsForBucket = _bucketIdToPartitionSegments.get(bucketId);
          if (partitionsForBucket != null) {
            NavigableSet<String> segmentsForPartition = partitionsForBucket.get(partitionId);
            if (segmentsForPartition != null) {
              segmentsForPartition.remove(segmentName);
              if (segmentsForPartition.isEmpty()) {
                partitionsForBucket.remove(partitionId);
              }
            }
            if (partitionsForBucket.isEmpty()) {
              _bucketIdToPartitionSegments.remove(bucketId);
            }
          }
        }
        _segmentToPartitionId.remove(segmentName);
        iterator.remove();
      }
    }

    // Add new segments (only these may require metadata lookups).
    for (String segmentName : onlineSegments) {
      if (_segmentToBucketId.containsKey(segmentName)) {
        continue;
      }
      long bucketId = getBucketIdForSegment(segmentName);
      if (bucketId < 0) {
        // Skip segments without parsable bucket id.
        continue;
      }
      int partitionId = getPartitionIdForSegment(segmentName);
      if (partitionId < 0) {
        // Skip segments without valid partition id for partition-aware sampling.
        continue;
      }
      _segmentToBucketId.put(segmentName, bucketId);
      _segmentToPartitionId.put(segmentName, partitionId);
      _bucketIdToPartitionSegments
          .computeIfAbsent(bucketId, k -> new HashMap<>())
          .computeIfAbsent(partitionId, k -> new TreeSet<>())
          .add(segmentName);
    }

    Set<String> selected = new HashSet<>();
    for (Map<Integer, NavigableSet<String>> partitionsForBucket : _bucketIdToPartitionSegments.values()) {
      for (NavigableSet<String> segmentsForPartition : partitionsForBucket.values()) {
        int count = 0;
        for (String segmentName : segmentsForPartition) {
          selected.add(segmentName);
          if (++count >= _numSegmentsPerBucket) {
            break;
          }
        }
      }
    }
    return selected;
  }

  private long getBucketId(long timeMs) {
    if (_useHourBuckets) {
      long epochHour = TimeUnit.MILLISECONDS.toHours(timeMs);
      return epochHour / _bucketHours;
    }
    ZonedDateTime zdt = Instant.ofEpochMilli(timeMs).atZone(ZoneOffset.UTC);
    long epochDay = zdt.toLocalDate().toEpochDay();
    return epochDay / _bucketDays;
  }

  private long getSegmentTimeMs(String segmentName) {
    if (_isRealtimeTable) {
      Long timeMs = getTimeMsFromRealtimeSegmentName(segmentName);
      return timeMs != null ? timeMs : -1L;
    }

    SegmentZKMetadata zkMetadata = ZKMetadataProvider.getSegmentZKMetadata(_propertyStore, _tableNameWithType,
        segmentName);
    if (zkMetadata == null) {
      return -1L;
    }
    return getEndTimeMs(zkMetadata);
  }

  private long getBucketIdForSegment(String segmentName) {
    long timeMs = getSegmentTimeMs(segmentName);
    return timeMs < 0 ? -1L : getBucketId(timeMs);
  }

  private int getPartitionIdForSegment(String segmentName) {
    if (!_usePartitionAware) {
      return DEFAULT_PARTITION_KEY;
    }
    SegmentZKMetadata zkMetadata = ZKMetadataProvider.getSegmentZKMetadata(_propertyStore, _tableNameWithType,
        segmentName);
    if (zkMetadata == null) {
      return -1;
    }
    SegmentPartitionMetadata partitionMetadata = zkMetadata.getPartitionMetadata();
    if (partitionMetadata == null) {
      return -1;
    }
    ColumnPartitionMetadata columnPartitionMetadata =
        partitionMetadata.getColumnPartitionMap().get(_partitionColumn);
    if (columnPartitionMetadata == null || columnPartitionMetadata.getNumPartitions() != _numPartitions) {
      return -1;
    }
    Set<Integer> partitions = columnPartitionMetadata.getPartitions();
    if (partitions == null || partitions.size() != 1) {
      return -1;
    }
    return partitions.iterator().next();
  }

  private static Long getTimeMsFromRealtimeSegmentName(String segmentName) {
    LLCSegmentName llcSegmentName = LLCSegmentName.of(segmentName);
    if (llcSegmentName != null) {
      return llcSegmentName.getCreationTimeMs();
    }
    UploadedRealtimeSegmentName uploadedRealtimeSegmentName = UploadedRealtimeSegmentName.of(segmentName);
    if (uploadedRealtimeSegmentName != null) {
      return parseRealtimeSegmentNameTime(uploadedRealtimeSegmentName.getCreationTime());
    }
    return null;
  }

  private static long parseRealtimeSegmentNameTime(String timeString) {
    try {
      return LocalDateTime.parse(timeString, REALTIME_SEGMENT_NAME_TIME_FORMATTER).toInstant(ZoneOffset.UTC)
          .toEpochMilli();
    } catch (DateTimeParseException e) {
      return -1L;
    }
  }

  private static long getEndTimeMs(SegmentZKMetadata zkMetadata) {
    long endTimeMs = zkMetadata.getEndTimeMs();
    if (endTimeMs > 0) {
      return endTimeMs;
    }
    // Handle explicit end time 0 (treated as invalid by SegmentZKMetadata#getEndTimeMs()).
    Map<String, String> simpleFields = zkMetadata.getSimpleFields();
    String endTimeString = simpleFields.get(Segment.END_TIME);
    if ("0".equals(endTimeString)) {
      String timeUnit = simpleFields.get(Segment.TIME_UNIT);
      if (timeUnit != null) {
        try {
          TimeUnit.valueOf(timeUnit);
        } catch (IllegalArgumentException e) {
          return -1L;
        }
        return 0L;
      }
    }
    return -1L;
  }
}
