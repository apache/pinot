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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.model.IdealState;
import org.apache.helix.store.HelixPropertyListener;
import org.apache.log4j.Logger;

import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.RealtimeDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.BrokerRequestUtils;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;
import com.linkedin.pinot.common.utils.SegmentNameBuilder;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotResourceIdealStateBuilder;


public class PinotRealtimeSegmentsManager implements HelixPropertyListener {
  private static final Logger logger = Logger.getLogger(PinotRealtimeSegmentsManager.class);

  private static final String REALTIME_SEGMENT_PROPERTY_STORE_PATH_PATTERN = "/SEGMENTS/.*_R|/SEGMENTS/.*_R/.*";

  private final PinotHelixResourceManager pinotClusterManager;

  public PinotRealtimeSegmentsManager(PinotHelixResourceManager pinotManager) {
    this.pinotClusterManager = pinotManager;
  }

  public void start() {
    logger.info("starting realtime segments manager, adding a listener on the property store root");
    this.pinotClusterManager.getPropertyStore().subscribe("/", this);
  }

  public void stop() {
    logger.info("stopping realtime segments manager, stopping property store");
    this.pinotClusterManager.getPropertyStore().stop();
  }

  private synchronized void eval() {
    // fetch current ideal state snapshot

    Map<String, IdealState> idealStateMap = new HashMap<String, IdealState>();

    for (String resource : pinotClusterManager.getAllRealtimeResources()) {
      idealStateMap.put(resource,
          pinotClusterManager.getHelixAdmin()
              .getResourceIdealState(pinotClusterManager.getHelixClusterName(), resource));
    }

    List<String> listOfSegmentsToAdd = new ArrayList<String>();

    for (String resource : idealStateMap.keySet()) {
      // get ideal state from map
      IdealState state = idealStateMap.get(resource);

      if (state.getPartitionSet().size() == 0) {
        // this is a brand new ideal state, which means we will add one new segment to every patition,replica
        List<String> instancesInResource =
            pinotClusterManager.getHelixAdmin().getInstancesInClusterWithTag(pinotClusterManager.getHelixClusterName(),
                resource);
        RealtimeDataResourceZKMetadata realtimeDRMetadata =
            pinotClusterManager.getRealtimeDataResourceZKMetadata(resource);
        String tableName = realtimeDRMetadata.getTableList().get(0);
        for (String instanceId : instancesInResource) {
          InstanceZKMetadata instanceZKMetadata = pinotClusterManager.getInstanceZKMetadata(instanceId);
          String groupId = instanceZKMetadata.getGroupId(resource);
          String partitionId = instanceZKMetadata.getPartition(resource);
          listOfSegmentsToAdd.add(SegmentNameBuilder.Realtime.build(resource, tableName, instanceId, groupId,
              partitionId, String.valueOf(System.currentTimeMillis())));
        }

      } else {
        Set<String> instancesToAssignRealtimeSegment = new HashSet<String>();
        instancesToAssignRealtimeSegment.addAll(pinotClusterManager.getHelixAdmin()
            .getInstancesInClusterWithTag(pinotClusterManager.getHelixClusterName(), resource));
        for (String partition : state.getPartitionSet()) {
          RealtimeSegmentZKMetadata realtimeSegmentZKMetadata =
              ZKMetadataProvider.getRealtimeSegmentZKMetadata(pinotClusterManager.getPropertyStore(),
                  SegmentNameBuilder.Realtime.extractResourceName(partition), partition);
          if (realtimeSegmentZKMetadata.getStatus() == Status.IN_PROGRESS) {
            String instanceName = SegmentNameBuilder.Realtime.extractInstanceName(partition);
            instancesToAssignRealtimeSegment.remove(instanceName);
          }
        }
        for (String instanceId : instancesToAssignRealtimeSegment) {
          InstanceZKMetadata instanceZKMetadata = pinotClusterManager.getInstanceZKMetadata(instanceId);
          String groupId = instanceZKMetadata.getGroupId(resource);
          String partitionId = instanceZKMetadata.getPartition(resource);
          RealtimeDataResourceZKMetadata realtimeDRMetadata =
              pinotClusterManager.getRealtimeDataResourceZKMetadata(resource);
          String tableName = realtimeDRMetadata.getTableList().get(0);
          listOfSegmentsToAdd.add(SegmentNameBuilder.Realtime.build(resource, tableName, instanceId, groupId,
              partitionId, String.valueOf(System.currentTimeMillis())));
        }
      }
    }

    logger.info("computed list of new segments to add : " + Arrays.toString(listOfSegmentsToAdd.toArray()));

    // new lets add the new segments
    for (String segmentId : listOfSegmentsToAdd) {
      String resourceName = SegmentNameBuilder.Realtime.extractResourceName(segmentId);
      String tableName = SegmentNameBuilder.Realtime.extractTableName(segmentId);
      String instanceName = SegmentNameBuilder.Realtime.extractInstanceName(segmentId);
      if (!idealStateMap.get(resourceName).getPartitionSet().contains(segmentId)) {
        // create realtime segment metadata
        RealtimeSegmentZKMetadata realtimeSegmentMetadataToAdd = new RealtimeSegmentZKMetadata();
        realtimeSegmentMetadataToAdd.setResourceName(BrokerRequestUtils.getHybridResourceName(resourceName));
        realtimeSegmentMetadataToAdd.setTableName(tableName);
        realtimeSegmentMetadataToAdd.setSegmentType(SegmentType.REALTIME);
        realtimeSegmentMetadataToAdd.setStatus(Status.IN_PROGRESS);
        realtimeSegmentMetadataToAdd.setSegmentName(segmentId);
        // add to property store first
        ZKMetadataProvider.setRealtimeSegmentZKMetadata(pinotClusterManager.getPropertyStore(), realtimeSegmentMetadataToAdd);
        //update ideal state next
        IdealState s =
            PinotResourceIdealStateBuilder.addNewRealtimeSegmentToIdealState(segmentId,
                idealStateMap.get(resourceName), instanceName);
        pinotClusterManager.getHelixAdmin().setResourceIdealState(pinotClusterManager.getHelixClusterName(),
            resourceName, PinotResourceIdealStateBuilder.addNewRealtimeSegmentToIdealState(segmentId, s, instanceName));
      }
    }
  }

  private boolean canEval() {
    return this.pinotClusterManager.isLeader();
  }

  @Override
  public synchronized void onDataChange(String path) {
    logger.info("**************************** : data changed : " + path);
    if (path.matches(REALTIME_SEGMENT_PROPERTY_STORE_PATH_PATTERN)) {
      if (canEval()) {
        eval();
      }
    } else {
      logger.info("Not matched data change path, do nothing");
    }
  }

  @Override
  public synchronized void onDataCreate(String path) {
    logger.info("**************************** : data create : " + path);
    if (path.matches(REALTIME_SEGMENT_PROPERTY_STORE_PATH_PATTERN)) {
      if (canEval()) {
        eval();
      }
    } else {
      logger.info("Not matched data create path, do nothing");
    }
  }

  @Override
  public synchronized void onDataDelete(String path) {
    logger.info("**************************** : data delete : " + path);
    if (path.matches(REALTIME_SEGMENT_PROPERTY_STORE_PATH_PATTERN)) {
      if (canEval()) {
        eval();
      }
    } else {
      logger.info("Not matched data delete path, do nothing");
    }
  }
}
