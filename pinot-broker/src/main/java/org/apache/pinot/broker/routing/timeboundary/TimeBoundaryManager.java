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
package org.apache.pinot.broker.routing.timeboundary;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code TimeBoundaryManager} class manages the time boundary information for a table.
 * <p>TODO: Support SDF (simple date format) time column
 */
public class TimeBoundaryManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeBoundaryManager.class);
  private static final long INVALID_END_TIME = -1;

  private final String _offlineTableName;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String _segmentZKMetadataPathPrefix;
  private final String _timeColumn;
  private final TimeUnit _timeUnit;
  private final boolean _isHourlyTable;
  private final Map<String, Long> _endTimeMap = new HashMap<>();

  private volatile TimeBoundaryInfo _timeBoundaryInfo;

  public TimeBoundaryManager(TableConfig tableConfig, ZkHelixPropertyStore<ZNRecord> propertyStore) {
    Preconditions.checkState(tableConfig.getTableType() == TableType.OFFLINE,
        "Cannot construct TimeBoundaryManager for real-time table: %s", tableConfig.getTableName());
    _offlineTableName = tableConfig.getTableName();
    _propertyStore = propertyStore;
    _segmentZKMetadataPathPrefix = ZKMetadataProvider.constructPropertyStorePathForResource(_offlineTableName) + "/";

    Schema schema = ZKMetadataProvider.getTableSchema(_propertyStore, _offlineTableName);
    Preconditions.checkState(schema != null, "Failed to find schema for table: %s", _offlineTableName);
    _timeColumn = tableConfig.getValidationConfig().getTimeColumnName();
    Preconditions
        .checkNotNull(_timeColumn, "Time column must be configured in table config for table: %s", _offlineTableName);
    DateTimeFieldSpec dateTimeSpec = schema.getSpecForTimeColumn(_timeColumn);
    Preconditions.checkNotNull(dateTimeSpec, "Field spec must be specified in schema for time column: %s of table: %s",
        _timeColumn, _offlineTableName);
    DateTimeFormatSpec formatSpec = new DateTimeFormatSpec(dateTimeSpec.getFormat());
    _timeUnit = formatSpec.getColumnUnit();
    Preconditions
        .checkNotNull(_timeUnit, "Time unit must be configured in the field spec for time column: %s of table: %s",
            _timeColumn, _offlineTableName);

    // For HOURLY table with time unit other than DAYS, use (maxEndTime - 1 HOUR) as the time boundary; otherwise, use
    // (maxEndTime - 1 DAY)
    _isHourlyTable = CommonConstants.Table.PUSH_FREQUENCY_HOURLY.equalsIgnoreCase(tableConfig.getValidationConfig().getSegmentPushFrequency())
        && _timeUnit != TimeUnit.DAYS;

    LOGGER.info("Constructed TimeBoundaryManager with timeColumn: {}, timeUnit: {}, isHourlyTable: {} for table: {}",
        _timeColumn, _timeUnit, _isHourlyTable, _offlineTableName);
  }

  /**
   * Initializes the time boundary manager with the external view and online segments (segments with ONLINE/CONSUMING
   * instances in ideal state). Should be called only once before calling other methods.
   * <p>NOTE: {@code externalView} is unused, but intentionally passed in as argument in case it is needed in the
   * future.
   */
  @SuppressWarnings("unused")
  public void init(ExternalView externalView, Set<String> onlineSegments) {
    // Bulk load time info for all online segments
    int numSegments = onlineSegments.size();
    List<String> segments = new ArrayList<>(numSegments);
    List<String> segmentZKMetadataPaths = new ArrayList<>(numSegments);
    for (String segment : onlineSegments) {
      segments.add(segment);
      segmentZKMetadataPaths.add(_segmentZKMetadataPathPrefix + segment);
    }
    List<ZNRecord> znRecords = _propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT);
    long maxEndTime = INVALID_END_TIME;
    for (int i = 0; i < numSegments; i++) {
      String segment = segments.get(i);
      long endTime = extractEndTimeFromSegmentZKMetadataZNRecord(segment, znRecords.get(i));
      _endTimeMap.put(segment, endTime);
      maxEndTime = Math.max(maxEndTime, endTime);
    }
    updateTimeBoundaryInfo(maxEndTime);
  }

  private long extractEndTimeFromSegmentZKMetadataZNRecord(String segment, @Nullable ZNRecord znRecord) {
    if (znRecord == null) {
      LOGGER.warn("Failed to find segment ZK metadata for segment: {}, table: {}", segment, _offlineTableName);
      return INVALID_END_TIME;
    }

    long endTime = znRecord.getLongField(CommonConstants.Segment.END_TIME, INVALID_END_TIME);
    if (endTime <= 0) {
      LOGGER.warn("Failed to find valid end time for segment: {}, table: {}", segment, _offlineTableName);
      return INVALID_END_TIME;
    }

    TimeUnit timeUnit = znRecord.getEnumField(CommonConstants.Segment.TIME_UNIT, TimeUnit.class, TimeUnit.DAYS);
    return _timeUnit.convert(endTime, timeUnit);
  }

  private void updateTimeBoundaryInfo(long maxEndTime) {
    if (maxEndTime > 0) {
      long timeBoundary = getTimeBoundary(maxEndTime);
      TimeBoundaryInfo currentTimeBoundaryInfo = _timeBoundaryInfo;
      if (currentTimeBoundaryInfo == null || Long.parseLong(currentTimeBoundaryInfo.getTimeValue()) != timeBoundary) {
        _timeBoundaryInfo = new TimeBoundaryInfo(_timeColumn, Long.toString(timeBoundary));
        LOGGER.info("Updated time boundary to: {} for table: {}", timeBoundary, _offlineTableName);
      }
    } else {
      LOGGER.warn("Failed to find segment with valid end time for table: {}, no time boundary generated",
          _offlineTableName);
      _timeBoundaryInfo = null;
    }
  }

  /**
   * Returns the time boundary based on the given maximum end time.
   * <p>NOTE: For HOURLY table with time unit other than DAYS, use (maxEndTime - 1 HOUR) as the time boundary;
   * otherwise, use (maxEndTime - 1 DAY).
   */
  private long getTimeBoundary(long maxEndTime) {
    if (_isHourlyTable) {
      return maxEndTime - _timeUnit.convert(1L, TimeUnit.HOURS);
    } else {
      return maxEndTime - _timeUnit.convert(1L, TimeUnit.DAYS);
    }
  }

  /**
   * Processes the external view change based on the given online segments (segments with ONLINE/CONSUMING instances in
   * ideal state).
   * <p>NOTE: We don't update all the segment ZK metadata for every external view change, but only the new added/removed
   * ones. The refreshed segment ZK metadata change won't be picked up.
   * <p>NOTE: {@code externalView} is unused, but intentionally passed in as argument in case it is needed in the
   * future.
   */
  @SuppressWarnings("unused")
  public synchronized void onExternalViewChange(ExternalView externalView, Set<String> onlineSegments) {
    for (String segment : onlineSegments) {
      _endTimeMap.computeIfAbsent(segment, k -> extractEndTimeFromSegmentZKMetadataZNRecord(segment,
          _propertyStore.get(_segmentZKMetadataPathPrefix + segment, null, AccessOption.PERSISTENT)));
    }
    _endTimeMap.keySet().retainAll(onlineSegments);
    updateTimeBoundaryInfo(getMaxEndTime());
  }

  private long getMaxEndTime() {
    long maxEndTime = INVALID_END_TIME;
    for (long endTime : _endTimeMap.values()) {
      maxEndTime = Math.max(maxEndTime, endTime);
    }
    return maxEndTime;
  }

  /**
   * Refreshes the metadata for the given segment (called when segment is getting refreshed).
   */
  public synchronized void refreshSegment(String segment) {
    _endTimeMap.put(segment, extractEndTimeFromSegmentZKMetadataZNRecord(segment,
        _propertyStore.get(_segmentZKMetadataPathPrefix + segment, null, AccessOption.PERSISTENT)));
    updateTimeBoundaryInfo(getMaxEndTime());
  }

  @Nullable
  public TimeBoundaryInfo getTimeBoundaryInfo() {
    return _timeBoundaryInfo;
  }
}
