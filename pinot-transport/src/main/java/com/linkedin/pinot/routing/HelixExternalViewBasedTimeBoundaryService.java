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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.store.zk.ZkHelixPropertyStore;

import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.resource.OfflineDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.OfflineSegmentZKMetadata;


public class HelixExternalViewBasedTimeBoundaryService implements TimeBoundaryService {

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
    List<OfflineSegmentZKMetadata> offlineSegmentZKMetadatas = ZKMetadataProvider.getOfflineResourceZKMetadataListForResource(_propertyStore, resourceName);
    OfflineDataResourceZKMetadata offlineDataResourceZKMetadata = ZKMetadataProvider.getOfflineResourceZKMetadata(_propertyStore, resourceName);
    TimeUnit resourceTimeUnit = getTimeUnitFromString(offlineDataResourceZKMetadata.getTimeType());

    if (offlineSegmentZKMetadatas.get(0).getTimeUnit() != null) {
      long maxTimeValue = -1;
      for (OfflineSegmentZKMetadata offlineSegmentZKMetadata : offlineSegmentZKMetadatas) {
        long endTime = resourceTimeUnit.convert(offlineSegmentZKMetadata.getEndTime(), offlineSegmentZKMetadata.getTimeUnit());
        if (maxTimeValue < endTime) {
          maxTimeValue = endTime;
        }
      }

      TimeBoundaryInfo timeBoundaryInfo = new TimeBoundaryInfo();
      offlineDataResourceZKMetadata.getTimeType();
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
