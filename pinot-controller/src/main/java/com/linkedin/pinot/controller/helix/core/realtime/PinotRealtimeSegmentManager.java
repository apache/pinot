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
package com.linkedin.pinot.controller.helix.core.realtime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.model.IdealState;
import org.apache.helix.store.HelixPropertyListener;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;
import com.linkedin.pinot.common.utils.SegmentNameBuilder;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotTableIdealStateBuilder;


/**
 * Realtime segment manager, which assigns realtime segments to server instances so that they can consume from Kafka.
 */
public class PinotRealtimeSegmentManager implements HelixPropertyListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotRealtimeSegmentManager.class);

  private static final String REALTIME_SEGMENT_PROPERTY_STORE_PATH_PATTERN =
      "/SEGMENTS/.*_REALTIME|/SEGMENTS/.*_REALTIME/.*";

  private final PinotHelixResourceManager pinotClusterManager;

  public PinotRealtimeSegmentManager(PinotHelixResourceManager pinotManager) {
    this.pinotClusterManager = pinotManager;
  }

  public void start() {
    LOGGER.info("Starting realtime segments manager, adding a listener on the property store root.");
    this.pinotClusterManager.getPropertyStore().subscribe("/", this);
  }

  public void stop() {
    LOGGER.info("Stopping realtime segments manager, stopping property store.");
    this.pinotClusterManager.getPropertyStore().stop();
  }

  private synchronized void assignRealtimeSegmentsToServerInstancesIfNecessary() throws JSONException, IOException {
    // Fetch current ideal state snapshot
    Map<String, IdealState> idealStateMap = new HashMap<String, IdealState>();

    for (String resource : pinotClusterManager.getAllRealtimeTables()) {
      idealStateMap.put(resource,
          pinotClusterManager.getHelixAdmin()
              .getResourceIdealState(pinotClusterManager.getHelixClusterName(), resource));
    }

    List<String> listOfSegmentsToAdd = new ArrayList<String>();

    for (String resource : idealStateMap.keySet()) {
      IdealState state = idealStateMap.get(resource);

      // Are there any partitions?
      if (state.getPartitionSet().size() == 0) {
        // No, this is a brand new ideal state, so we will add one new segment to every partition and replica
        List<String> instancesInResource = new ArrayList<String>();
        try {
          instancesInResource.addAll(pinotClusterManager.getServerInstancesForTable(resource, TableType.REALTIME));
        } catch (Exception e) {
          LOGGER.error("Caught exception while fetching instances for resource {}", resource, e);
        }

        // Assign a new segment to all server instances
        for (String instanceId : instancesInResource) {
          InstanceZKMetadata instanceZKMetadata = pinotClusterManager.getInstanceZKMetadata(instanceId);
          String groupId = instanceZKMetadata.getGroupId(resource);
          String partitionId = instanceZKMetadata.getPartition(resource);
          listOfSegmentsToAdd.add(SegmentNameBuilder.Realtime.build(resource, instanceId, groupId, partitionId,
              String.valueOf(System.currentTimeMillis())));
        }
      } else {
        // Add all server instances to the list of instances for which to assign a realtime segment
        Set<String> instancesToAssignRealtimeSegment = new HashSet<String>();
        instancesToAssignRealtimeSegment.addAll(pinotClusterManager.getServerInstancesForTable(resource,
            TableType.REALTIME));

        // Remove server instances that are currently processing a segment
        for (String partition : state.getPartitionSet()) {
          RealtimeSegmentZKMetadata realtimeSegmentZKMetadata =
              ZKMetadataProvider.getRealtimeSegmentZKMetadata(pinotClusterManager.getPropertyStore(),
                  SegmentNameBuilder.Realtime.extractTableName(partition), partition);
          if (realtimeSegmentZKMetadata.getStatus() == Status.IN_PROGRESS) {
            String instanceName = SegmentNameBuilder.Realtime.extractInstanceName(partition);
            instancesToAssignRealtimeSegment.remove(instanceName);
          }
        }

        // Assign a new segment to the server instances not currently processing this segment
        for (String instanceId : instancesToAssignRealtimeSegment) {
          InstanceZKMetadata instanceZKMetadata = pinotClusterManager.getInstanceZKMetadata(instanceId);
          String groupId = instanceZKMetadata.getGroupId(resource);
          String partitionId = instanceZKMetadata.getPartition(resource);
          listOfSegmentsToAdd.add(SegmentNameBuilder.Realtime.build(resource, instanceId, groupId, partitionId,
              String.valueOf(System.currentTimeMillis())));
        }
      }
    }

    LOGGER.info("Computed list of new segments to add : " + Arrays.toString(listOfSegmentsToAdd.toArray()));

    // Add the new segments to the server instances
    for (String segmentId : listOfSegmentsToAdd) {
      String resourceName = SegmentNameBuilder.Realtime.extractTableName(segmentId);
      String instanceName = SegmentNameBuilder.Realtime.extractInstanceName(segmentId);

      // Does the ideal state already contain this segment?
      if (!idealStateMap.get(resourceName).getPartitionSet().contains(segmentId)) {
        // No, add it
        // Create the realtime segment metadata
        RealtimeSegmentZKMetadata realtimeSegmentMetadataToAdd = new RealtimeSegmentZKMetadata();
        realtimeSegmentMetadataToAdd.setTableName(TableNameBuilder.extractRawTableName(resourceName));
        realtimeSegmentMetadataToAdd.setSegmentType(SegmentType.REALTIME);
        realtimeSegmentMetadataToAdd.setStatus(Status.IN_PROGRESS);
        realtimeSegmentMetadataToAdd.setSegmentName(segmentId);

        // Add the new metadata to the property store
        ZKMetadataProvider.setRealtimeSegmentZKMetadata(pinotClusterManager.getPropertyStore(),
            realtimeSegmentMetadataToAdd);

        // Update the ideal state to add the new realtime segment
        IdealState s =
            PinotTableIdealStateBuilder.addNewRealtimeSegmentToIdealState(segmentId, idealStateMap.get(resourceName),
                instanceName);

        pinotClusterManager.getHelixAdmin().setResourceIdealState(pinotClusterManager.getHelixClusterName(),
            resourceName, PinotTableIdealStateBuilder.addNewRealtimeSegmentToIdealState(segmentId, s, instanceName));
      }
    }
  }

  private boolean isLeader() {
    return this.pinotClusterManager.isLeader();
  }

  @Override
  public synchronized void onDataChange(String path) {
    processPropertyStoreChange(path);
  }

  @Override
  public synchronized void onDataCreate(String path) {
    processPropertyStoreChange(path);
  }

  @Override
  public synchronized void onDataDelete(String path) {
    processPropertyStoreChange(path);
  }

  private void processPropertyStoreChange(String path) {
    try {
      if (path.matches(REALTIME_SEGMENT_PROPERTY_STORE_PATH_PATTERN)) {
        if (isLeader()) {
          assignRealtimeSegmentsToServerInstancesIfNecessary();
        } else {
          LOGGER.info("Not the leader of this cluster, ignoring realtime segment property store change.");
        }
      } else {
        LOGGER.info("Path {} does not match a realtime segment, ignoring.", path);
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while processing data change for path {}", path, e);
      Utils.rethrowException(e);
    }
  }
}
