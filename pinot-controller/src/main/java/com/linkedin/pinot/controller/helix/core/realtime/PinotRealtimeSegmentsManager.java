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
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.ZkClient;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.HelixPropertyListener;
import org.apache.log4j.Logger;

import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.RealtimeDataResourceZKMetadata;
import com.linkedin.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import com.linkedin.pinot.common.utils.CommonConstants.Segment.SegmentType;
import com.linkedin.pinot.common.utils.SegmentNameBuilder;
import com.linkedin.pinot.controller.helix.core.HelixHelper;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.PinotResourceIdealStateBuilder;
import com.linkedin.pinot.controller.helix.core.utils.PinotHelixUtils;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;


public class PinotRealtimeSegmentsManager implements HelixPropertyListener {
  private static final Logger logger = Logger.getLogger(PinotRealtimeSegmentsManager.class);

  private final HelixManager helixManager;
  private final PinotHelixResourceManager pinotClusterManager;

  public PinotRealtimeSegmentsManager(PinotHelixResourceManager pinotManager) {
    this.pinotClusterManager = pinotManager;
    this.helixManager = this.pinotClusterManager.getHelixZkManager();
  }

  public void start() {
    this.pinotClusterManager.getPropertyStore().subscribe("/", this);
  }

  private void eval() {
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
          InstanceZKMetadata m =
              new InstanceZKMetadata(pinotClusterManager.getHelixAdmin()
                  .getInstanceConfig(pinotClusterManager.getHelixClusterName(), instanceId).getRecord());
          String groupId = m.getGroupId(resource);
          String partitionId = m.getPartition(resource);
          listOfSegmentsToAdd.add(SegmentNameBuilder.Realtime.build(resource, tableName, instanceId, groupId,
              partitionId, String.valueOf(System.currentTimeMillis())));
        }

      } else {
        for (String partition : state.getPartitionSet()) {
          assert (1 == state.getInstanceSet(partition).size());
          RealtimeSegmentZKMetadata m =
              HelixHelper.getRealtimeSegmentZKMetadata(pinotClusterManager.getPropertyStore(),
                  SegmentNameBuilder.Realtime.extractResourceName(partition), partition);
          if (m != null && m.getStatus() == Status.DONE) {
            // time to create a new Segment,
            // status done means the combination of (instance, group, partition) is ready to accept a new segment
            String resourceName = SegmentNameBuilder.Realtime.extractResourceName(partition);
            String tableName = SegmentNameBuilder.Realtime.extractTableName(partition);
            String instanceName = SegmentNameBuilder.Realtime.extractInstanceName(partition);
            String groupId = SegmentNameBuilder.Realtime.extractGroupIdName(partition);
            String partitionId = SegmentNameBuilder.Realtime.extractPartitionName(partition);
            String sequenceNumber = String.valueOf(System.currentTimeMillis());
            listOfSegmentsToAdd.add(SegmentNameBuilder.Realtime.build(resourceName, tableName, instanceName, groupId,
                partitionId, sequenceNumber));
          } else {
            logger.info("partition : " + partition + " is still in progress");
          }
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
        realtimeSegmentMetadataToAdd.setResourceName(resourceName);
        realtimeSegmentMetadataToAdd.setTableName(tableName);
        realtimeSegmentMetadataToAdd.setSegmentType(SegmentType.REALTIME);
        realtimeSegmentMetadataToAdd.setStatus(Status.IN_PROGRESS);
        ZNRecord rec = realtimeSegmentMetadataToAdd.toZNRecord();
        // add to property store first
        pinotClusterManager.getPropertyStore().create("/" + resourceName + "/" + segmentId, rec,
            AccessOption.PERSISTENT);
        //update ideal state next
        IdealState s = idealStateMap.get(resourceName);
        pinotClusterManager.getHelixAdmin().setResourceIdealState(pinotClusterManager.getHelixClusterName(),
            resourceName, PinotResourceIdealStateBuilder.addNewRealtimeSegmentToIdealState(segmentId, s, instanceName));
      }
    }
  }

  @Override
  public synchronized void onDataChange(String path) {
    logger.info("**************************** : data changed : " + path);
    eval();
  }

  @Override
  public synchronized void onDataCreate(String path) {
    logger.info("**************************** : data create : " + path);
    eval();
  }

  @Override
  public synchronized void onDataDelete(String path) {
    logger.info("**************************** : data delete : " + path);
    eval();
  }

  public static void main(String[] args) throws Exception {

    final String helixClusterName = "testingRealtime";

    ZkClient client = new ZkClient("localhost:2181");
    if (client.exists("/" + helixClusterName)) {
      client.deleteRecursive("/" + helixClusterName);
    }

    client.close();

    PinotHelixResourceManager pinotManager =
        new PinotHelixResourceManager("localhost:2181", helixClusterName, "localhost_21212", "/tmp");
    pinotManager.start();

    PinotRealtimeSegmentsManager segmentsManager = new PinotRealtimeSegmentsManager(pinotManager);
    segmentsManager.start();

    int c = 0;
    String[] resources =
        { "resource1_O", "resource1_R", "resource2_O", "resource2_R", "resource3_R", "resource4_O", "resource4_R", "resource5_O" };

    for (String resourceName : resources) {
      pinotManager.getPropertyStore().create("/" + resourceName, new ZNRecord(resourceName), AccessOption.PERSISTENT);
      System.out.println("****************** : " + resourceName);
      if (resourceName.indexOf("_O") != -1) {
        String segmentPath =
            PinotHelixUtils.constructPropertyStorePathForSegment(resourceName, resourceName + "_Segment_" + c);
        final ZNRecord record = new ZNRecord(resourceName + "_Segment_" + c);
        record.setSimpleField(V1Constants.SEGMENT_DOWNLOAD_URL, "http://some");
        record.setSimpleField(V1Constants.SEGMENT_PUSH_TIME, String.valueOf(System.currentTimeMillis()));
        pinotManager.getPropertyStore().create("/" + resourceName + "/" + resourceName + "_Segment_" + c, record,
            AccessOption.PERSISTENT);
      }
    }

    Thread.sleep(1000);

  }
}
