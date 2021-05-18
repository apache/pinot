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
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code TimeBoundaryManager} class manages the time boundary information for a table.
 * <p>TODO: Support SDF (simple date format) time column
 */
public class TimeBoundaryManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeBoundaryManager.class);
  private static final long INVALID_END_TIME_MS = -1;

  private final String _offlineTableName;
  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final String _segmentZKMetadataPathPrefix;
  private final String _timeColumn;
  private final DateTimeFormatSpec _timeFormatSpec;
  private final long _timeOffsetMs;
  private final Map<String, Long> _endTimeMsMap = new HashMap<>();

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
    _timeFormatSpec = new DateTimeFormatSpec(dateTimeSpec.getFormat());
    Preconditions.checkNotNull(_timeFormatSpec.getColumnUnit(),
        "Time unit must be configured in the field spec for time column: %s of table: %s", _timeColumn,
        _offlineTableName);

    // For HOURLY table with time unit other than DAYS, use (maxEndTime - 1 HOUR) as the time boundary; otherwise, use
    // (maxEndTime - 1 DAY)
    boolean isHourlyTable = CommonConstants.Table.PUSH_FREQUENCY_HOURLY
        .equalsIgnoreCase(IngestionConfigUtils.getBatchSegmentIngestionFrequency(tableConfig))
        && _timeFormatSpec.getColumnUnit() != TimeUnit.DAYS;
    _timeOffsetMs = isHourlyTable ? TimeUnit.HOURS.toMillis(1) : TimeUnit.DAYS.toMillis(1);

    LOGGER.info("Constructed TimeBoundaryManager with timeColumn: {}, timeFormat: {}, isHourlyTable: {} for table: {}",
        _timeColumn, _timeFormatSpec.getFormat(), isHourlyTable, _offlineTableName);
  }

  /**
   * Initializes the time boundary manager with the external view, ideal state and online segments (segments with
   * ONLINE/CONSUMING instances in the ideal state and selected by the pre-selector). Should be called only once before
   * calling other methods.
   * <p>NOTE: {@code externalView} and {@code idealState} are unused, but intentionally passed in in case they are
   * needed in the future.
   */
  @SuppressWarnings("unused")
  public void init(ExternalView externalView, IdealState idealState, Set<String> onlineSegments) {
    // Bulk load time info for all online segments
    int numSegments = onlineSegments.size();
    List<String> segments = new ArrayList<>(numSegments);
    List<String> segmentZKMetadataPaths = new ArrayList<>(numSegments);
    for (String segment : onlineSegments) {
      segments.add(segment);
      segmentZKMetadataPaths.add(_segmentZKMetadataPathPrefix + segment);
    }
    List<ZNRecord> znRecords = _propertyStore.get(segmentZKMetadataPaths, null, AccessOption.PERSISTENT);
    long maxEndTimeMs = INVALID_END_TIME_MS;
    for (int i = 0; i < numSegments; i++) {
      String segment = segments.get(i);
      long endTimeMs = extractEndTimeMsFromSegmentZKMetadataZNRecord(segment, znRecords.get(i));
      _endTimeMsMap.put(segment, endTimeMs);
      maxEndTimeMs = Math.max(maxEndTimeMs, endTimeMs);
    }
    updateTimeBoundaryInfo(maxEndTimeMs);
  }

  private long extractEndTimeMsFromSegmentZKMetadataZNRecord(String segment, @Nullable ZNRecord znRecord) {
    if (znRecord == null) {
      LOGGER.warn("Failed to find segment ZK metadata for segment: {}, table: {}", segment, _offlineTableName);
      return INVALID_END_TIME_MS;
    }
    long totalDocs = znRecord.getLongField(CommonConstants.Segment.TOTAL_DOCS, -1);
    long endTimeMs = INVALID_END_TIME_MS;
    if (totalDocs != 0) {
      long endTime = znRecord.getLongField(CommonConstants.Segment.END_TIME, -1);
      if (endTime > 0) {
        TimeUnit timeUnit = znRecord.getEnumField(CommonConstants.Segment.TIME_UNIT, TimeUnit.class, TimeUnit.DAYS);
        endTimeMs = timeUnit.toMillis(endTime);
      } else {
        LOGGER.warn("Failed to find valid end time for segment: {}, table: {}", segment, _offlineTableName);
      }
    }
    return endTimeMs;
  }

  private void updateTimeBoundaryInfo(long maxEndTimeMs) {
    if (maxEndTimeMs > 0) {
      String timeBoundary = _timeFormatSpec.fromMillisToFormat(maxEndTimeMs - _timeOffsetMs);
      TimeBoundaryInfo currentTimeBoundaryInfo = _timeBoundaryInfo;
      if (currentTimeBoundaryInfo == null || !currentTimeBoundaryInfo.getTimeValue().equals(timeBoundary)) {
        _timeBoundaryInfo = new TimeBoundaryInfo(_timeColumn, timeBoundary);
        LOGGER.info("Updated time boundary to: {} for table: {}", timeBoundary, _offlineTableName);
      }
    } else {
      LOGGER.warn("Failed to find segment with valid end time for table: {}, no time boundary generated",
          _offlineTableName);
      _timeBoundaryInfo = null;
    }
  }

  /**
   * Processes the external view change based on the given ideal state and online segments (segments with
   * ONLINE/CONSUMING instances in the ideal state and selected by the pre-selector).
   * <p>NOTE: We don't update all the segment ZK metadata for every external view change, but only the new added/removed
   * ones. The refreshed segment ZK metadata change won't be picked up.
   * <p>NOTE: {@code idealState} is unused, but intentionally passed in in case it is needed in the future.
   */
  @SuppressWarnings("unused")
  public synchronized void onExternalViewChange(ExternalView externalView, IdealState idealState,
      Set<String> onlineSegments) {
    for (String segment : onlineSegments) {
      // NOTE: Only update the segment end time when there are ONLINE instances in the external view to prevent moving
      //       the time boundary before the new segment is picked up by the servers
      Map<String, String> instanceStateMap = externalView.getStateMap(segment);
      if (instanceStateMap != null && instanceStateMap.containsValue(SegmentStateModel.ONLINE)) {
        _endTimeMsMap.computeIfAbsent(segment, k -> extractEndTimeMsFromSegmentZKMetadataZNRecord(segment,
            _propertyStore.get(_segmentZKMetadataPathPrefix + segment, null, AccessOption.PERSISTENT)));
      }
    }
    _endTimeMsMap.keySet().retainAll(onlineSegments);
    updateTimeBoundaryInfo(getMaxEndTimeMs());
  }

  private long getMaxEndTimeMs() {
    long maxEndTimeMs = INVALID_END_TIME_MS;
    for (long endTimeMs : _endTimeMsMap.values()) {
      maxEndTimeMs = Math.max(maxEndTimeMs, endTimeMs);
    }
    return maxEndTimeMs;
  }

  /**
   * Refreshes the metadata for the given segment (called when segment is getting refreshed).
   */
  public synchronized void refreshSegment(String segment) {
    _endTimeMsMap.put(segment, extractEndTimeMsFromSegmentZKMetadataZNRecord(segment,
        _propertyStore.get(_segmentZKMetadataPathPrefix + segment, null, AccessOption.PERSISTENT)));
    updateTimeBoundaryInfo(getMaxEndTimeMs());
  }

  @Nullable
  public TimeBoundaryInfo getTimeBoundaryInfo() {
    return _timeBoundaryInfo;
  }
}
