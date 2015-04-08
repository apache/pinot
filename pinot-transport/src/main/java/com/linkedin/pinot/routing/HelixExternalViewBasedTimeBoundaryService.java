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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.log4j.Logger;

import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.resource.OfflineDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.ResourceType;


public class HelixExternalViewBasedTimeBoundaryService implements TimeBoundaryService {

  private final Logger LOGGER = Logger.getLogger(HelixExternalViewBasedTimeBoundaryService.class);

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
    String resourceName = externalView.getResourceName();
    // Do nothing for Realtime Resource.
    if (BrokerRequestUtils.getResourceTypeFromResourceName(resourceName) == ResourceType.REALTIME) {
      return;
    }
    Set<String> offlineSegmentsServing = externalView.getPartitionSet();
    OfflineDataResourceZKMetadata offlineDataResourceZKMetadata = ZKMetadataProvider.getOfflineResourceZKMetadata(_propertyStore, resourceName);
    TimeUnit resourceTimeUnit = getTimeUnitFromString(offlineDataResourceZKMetadata.getTimeType());
    if (!offlineSegmentsServing.isEmpty() && resourceTimeUnit != null) {
      long maxTimeValue = -1;
      for (String segmentId : offlineSegmentsServing) {
        try {
          long endTime = -1;
          OfflineSegmentZKMetadata offlineSegmentZKMetadata = ZKMetadataProvider.getOfflineSegmentZKMetadata(_propertyStore, resourceName, segmentId);
          if (offlineSegmentZKMetadata.getEndTime() > 0) {
            if (offlineSegmentZKMetadata.getTimeUnit() != null) {
              endTime = resourceTimeUnit.convert(offlineSegmentZKMetadata.getEndTime(), offlineSegmentZKMetadata.getTimeUnit());
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
      timeBoundaryInfo.setTimeColumn(offlineDataResourceZKMetadata.getTimeColumnName());
      timeBoundaryInfo.setTimeValue(Long.toString(maxTimeValue));
      _timeBoundaryInfoMap.put(resourceName, timeBoundaryInfo);
    }
  }

  private TimeUnit getTimeUnitFromString(String timeTypeString) {
    TimeUnit timeUnit = null;
    try {
      timeUnit = TimeUnit.valueOf(timeTypeString);
    } catch (Exception e) {
    }
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
  public void remove(String resourceName) {
    _timeBoundaryInfoMap.remove(resourceName);

  }

  @Override
  public TimeBoundaryInfo getTimeBoundaryInfoFor(String resource) {
    return _timeBoundaryInfoMap.get(resource);
  }

}
