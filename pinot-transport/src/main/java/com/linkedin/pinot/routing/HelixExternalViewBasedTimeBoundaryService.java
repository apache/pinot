/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.routing;

import com.linkedin.pinot.common.utils.time.TimeUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.config.AbstractTableConfig;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;


public class HelixExternalViewBasedTimeBoundaryService implements TimeBoundaryService {

  private static final Logger LOGGER = LoggerFactory.getLogger(HelixExternalViewBasedTimeBoundaryService.class);

  private static final String DAYS_SINCE_EPOCH = "daysSinceEpoch";
  private static final String HOURS_SINCE_EPOCH = "hoursSinceEpoch";
  private static final String MINUTES_SINCE_EPOCH = "minutesSinceEpoch";
  private static final String SECONDS_SINCE_EPOCH = "secondsSinceEpoch";

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final Map<String, TimeBoundaryInfo> _timeBoundaryInfoMap = new HashMap<String, TimeBoundaryInfo>();

  public HelixExternalViewBasedTimeBoundaryService(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;
  }

  public synchronized void updateTimeBoundaryService(ExternalView externalView) {
    if (_propertyStore == null) {
      return;
    }
    String tableName = externalView.getResourceName();
    // Do nothing for realtime table.
    if (TableNameBuilder.getTableTypeFromTableName(tableName) == TableType.REALTIME) {
      return;
    }
    Set<String> offlineSegmentsServing = externalView.getPartitionSet();
    AbstractTableConfig offlineTableConfig = ZKMetadataProvider.getOfflineTableConfig(_propertyStore, tableName);

    TimeUnit tableTimeUnit = getTimeUnitFromString(offlineTableConfig.getValidationConfig().getTimeType());
    if (!offlineSegmentsServing.isEmpty() && tableTimeUnit != null) {
      long maxTimeValue = -1;
      for (String segmentId : offlineSegmentsServing) {
        try {
          long endTime = -1;
          OfflineSegmentZKMetadata offlineSegmentZKMetadata = ZKMetadataProvider.getOfflineSegmentZKMetadata(_propertyStore, tableName, segmentId);
          if (offlineSegmentZKMetadata.getEndTime() > 0) {
            if (offlineSegmentZKMetadata.getTimeUnit() != null) {
              endTime = tableTimeUnit.convert(offlineSegmentZKMetadata.getEndTime(), offlineSegmentZKMetadata.getTimeUnit());
            } else {
              endTime = offlineSegmentZKMetadata.getEndTime();
            }
          }
          maxTimeValue = Math.max(maxTimeValue, endTime);
        } catch (Exception e) {
          LOGGER.error("Error during convert end time for segment - " + segmentId + ", exceptions: " + e);
        }
      }
      TimeBoundaryInfo timeBoundaryInfo = new TimeBoundaryInfo();
      timeBoundaryInfo.setTimeColumn(offlineTableConfig.getValidationConfig().getTimeColumnName());
      timeBoundaryInfo.setTimeValue(Long.toString(maxTimeValue));
      _timeBoundaryInfoMap.put(tableName, timeBoundaryInfo);
    }
  }

  private TimeUnit getTimeUnitFromString(String timeTypeString) {
    // If input data does not have a time column, no need to fire an exception.
    if ((timeTypeString == null) || timeTypeString.isEmpty()) {
      return null;
    }

    TimeUnit timeUnit = TimeUtils.timeUnitFromString(timeTypeString);

    // Check legacy time formats
    if (timeUnit == null) {
      if (timeTypeString.equalsIgnoreCase(DAYS_SINCE_EPOCH)) {
        timeUnit = TimeUnit.DAYS;
      }
      if (timeTypeString.equalsIgnoreCase(HOURS_SINCE_EPOCH)) {
        timeUnit = TimeUnit.HOURS;
      }
      if (timeTypeString.equalsIgnoreCase(MINUTES_SINCE_EPOCH)) {
        timeUnit = TimeUnit.MINUTES;
      }
      if (timeTypeString.equalsIgnoreCase(SECONDS_SINCE_EPOCH)) {
        timeUnit = TimeUnit.SECONDS;
      }
    }

    if (timeUnit == null) {
      throw new RuntimeException("Not supported time type for: " + timeTypeString);
    }
    return timeUnit;
  }

  @Override
  public void remove(String tableName) {
    _timeBoundaryInfoMap.remove(tableName);

  }

  @Override
  public TimeBoundaryInfo getTimeBoundaryInfoFor(String table) {
    return _timeBoundaryInfoMap.get(table);
  }

}
