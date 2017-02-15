/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.server.starter.helix;

import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.ServiceStatus;
import com.linkedin.pinot.core.data.manager.offline.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManager;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Callback to determine whether or not a server is done starting up.
 */
public class ServerServiceStatusCallback implements ServiceStatus.ServiceStatusCallback {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerServiceStatusCallback.class);

  private final HelixAdmin _helixAdmin;
  private final InstanceDataManager _instanceDataManager;
  private final String _clusterName;
  private final String _instanceName;
  private boolean _finishedStartingUp;

  public ServerServiceStatusCallback(HelixAdmin helixAdmin, InstanceDataManager instanceDataManager, String clusterName,
      String instanceName) {
    _helixAdmin = helixAdmin;
    _instanceDataManager = instanceDataManager;
    _clusterName = clusterName;
    _instanceName = instanceName;
  }

  @Override
  public synchronized ServiceStatus.Status getServiceStatus() {
    if (_finishedStartingUp) {
      return ServiceStatus.Status.GOOD;
    }

    // Figure out the serving status of all tables we're supposed to serve
    List<String> resourcesInCluster = _helixAdmin.getResourcesInCluster(_clusterName);
    Set<String> tablesToServe = new HashSet<>();
    Set<String> tablesNotServed = new HashSet<>();
    Set<String> tablesPartiallyServed = new HashSet<>();
    Set<String> tablesServed = new HashSet<>();
    int segmentsRemainingToLoad = 0;
    int servableSegmentCount = 0;

    for (String tableName : resourcesInCluster) {
      // Skip brokerResource
      if (CommonConstants.Helix.NON_PINOT_RESOURCE_RESOURCE_NAMES.contains(tableName)) {
        continue;
      }

      // Fetch ideal state
      IdealState idealState = _helixAdmin.getResourceIdealState(_clusterName, tableName);
      if (idealState == null) {
        continue;
      }

      // Gather segments to serve from the ideal state
      Set<String> segmentsToServe = new HashSet<>();
      for (String segmentName : idealState.getPartitionSet()) {
        String segmentState = idealState.getInstanceStateMap(segmentName).get(_instanceName);

        if (segmentState != null && !"OFFLINE".equals(segmentState) && !"DROPPED".equals(segmentState)) {
          tablesToServe.add(tableName);
          segmentsToServe.add(segmentName);
          servableSegmentCount++;
        }
      }

      // If we have no segments to serve, ignore this table
      if (segmentsToServe.isEmpty()) {
        continue;
      }

      // Compare the list of segments to serve with what is actually being served
      Set<String> segmentsNotServed = new HashSet<>(segmentsToServe);
      TableDataManager tableDataManager = _instanceDataManager.getTableDataManager(tableName);
      if (tableDataManager != null) {
        List<SegmentDataManager> segments = tableDataManager.acquireAllSegments();
        for (SegmentDataManager segment : segments) {
          try {
            segmentsNotServed.remove(segment.getSegmentName());
          } finally {
            tableDataManager.releaseSegment(segment);
          }
        }
      }

      segmentsRemainingToLoad += segmentsNotServed.size();

      if (segmentsToServe.size() == segmentsNotServed.size()) {
        tablesNotServed.add(tableName);
      } else if (!segmentsNotServed.isEmpty()) {
        tablesPartiallyServed.add(tableName);
      } else {
        tablesServed.add(tableName);
      }
    }

    if (tablesNotServed.isEmpty() && tablesPartiallyServed.isEmpty()) {
      LOGGER.info("Instance {} is done starting up, {} tables served", _instanceName, tablesServed.size());
      _finishedStartingUp = true;
      return ServiceStatus.Status.GOOD;
    } else {
      LOGGER.info(
          "Instance {} is still starting up, {}/{} segments left to load. Out of {} tables to load, {} tables are fully loaded, {} tables are partially loaded and {} tables are not loaded at all",
          _instanceName, segmentsRemainingToLoad, servableSegmentCount, tablesToServe.size(), tablesServed.size(),
          tablesPartiallyServed.size(), tablesNotServed.size());
      return ServiceStatus.Status.STARTING;
    }
  }
}
