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

import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.ExternalView;
import org.apache.helix.store.zk.ZkHelixPropertyStore;

import com.linkedin.pinot.common.utils.BrokerRequestUtils;


public class HelixExternalViewBasedTimeBoundaryService implements TimeBoundaryService {

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final Map<String, TimeBoundaryInfo> _timeBoundaryInfoMap = new HashMap<String, TimeBoundaryInfo>();
  private final static String SEGMENT_TIME_COLUMN = "segment.time.column.name";
  private final static String SEGMENT_END_TIME = "segment.end.time";

  public HelixExternalViewBasedTimeBoundaryService(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;
  }

  public synchronized void updateTimeBoundaryService(ExternalView externalView) {
    if (_propertyStore == null) {
      return;
    }
    String resourceName = externalView.getResourceName();
    List<ZNRecord> segmentList = _propertyStore.getChildren("/" + BrokerRequestUtils.getOfflineResourceNameForResource(resourceName), null, AccessOption.PERSISTENT);
    if (segmentList.get(0).getSimpleFields().containsKey(SEGMENT_TIME_COLUMN) &&
        segmentList.get(0).getSimpleFields().containsKey(SEGMENT_END_TIME)) {
      long maxTimeValue = -1;
      for (ZNRecord segmentRecord : segmentList) {
        long endTime = segmentRecord.getLongField(SEGMENT_END_TIME, -1);
        if (maxTimeValue < endTime) {
          maxTimeValue = endTime;
        }
      }

      TimeBoundaryInfo timeBoundaryInfo = new TimeBoundaryInfo();
      timeBoundaryInfo.setTimeColumn(segmentList.get(0).getSimpleField(SEGMENT_TIME_COLUMN));
      timeBoundaryInfo.setTimeValue(maxTimeValue + "");

      _timeBoundaryInfoMap.put(resourceName, timeBoundaryInfo);
    }
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
