/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.thirdeye.hadoop.push;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.IOUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteOverlappingSegmentsInPinot {

  private static final Logger LOG = LoggerFactory.getLogger(DeleteOverlappingSegmentsInPinot.class);

  public static void main(String[] args) throws Exception {
    String zkUrl = args[0];
    String zkCluster = args[1];
    String tableName = args[2];
    deleteOverlappingSegments(zkUrl, zkCluster, tableName);
  }

  private static IdealState computeNewIdealStateAfterDeletingOverlappingSegments(HelixDataAccessor helixDataAccessor, PropertyKey idealStatesKey) {
    IdealState is = helixDataAccessor.getProperty(idealStatesKey);
    // compute existing DAILY segments
    Set<String> daysWithDailySegments = new HashSet<>();
    for (String segmentName : is.getPartitionSet()) {
      LOG.info("Segment Name : {}", segmentName);
      if (segmentName.indexOf("DAILY") > -1) {
        String[] splits = segmentName.split("_");
        String endDay = splits[splits.length - 2].substring(0, "yyyy-mm-dd".length());
        String startDay = splits[splits.length - 3].substring(0, "yyyy-mm-dd".length());
        LOG.info("Start : {} End : {}", startDay, endDay);
        daysWithDailySegments.add(startDay);
      }
    }
    // compute list of HOURLY segments to be deleted
    Set<String> hourlySegmentsToDelete = new TreeSet<>();
    for (String segmentName : is.getPartitionSet()) {
      LOG.info("Segment name {}", segmentName);
      if (segmentName.indexOf("HOURLY") > -1) {
        String[] splits = segmentName.split("_");
        String endDay = splits[splits.length - 2].substring(0, "yyyy-mm-dd".length());
        String startDay = splits[splits.length - 3].substring(0, "yyyy-mm-dd".length());
        LOG.info("Start : {} End : {}", startDay, endDay);
        if (daysWithDailySegments.contains(startDay)) {
          hourlySegmentsToDelete.add(segmentName);
        }
      }
    }
    LOG.info("HOURLY segments that can be deleted: {}", hourlySegmentsToDelete.size());
    LOG.info("Hourly segments to delete {}", hourlySegmentsToDelete.toString().replaceAll(",", "\n"));
    IdealState newIdealState = new IdealState(is.getRecord());
    for (String hourlySegmentToDelete : hourlySegmentsToDelete) {
      newIdealState.getRecord().getMapFields().remove(hourlySegmentToDelete);
    }
    return newIdealState;
  }

  public static boolean deleteOverlappingSegments(String zkUrl, String zkCluster, String tableName) {
    boolean updateSuccessful = false;

    if (!tableName.endsWith("_OFFLINE")) {
      tableName = tableName + "_OFFLINE";
    }

    ZkClient zkClient = new ZkClient(zkUrl);
    ZNRecordSerializer zkSerializer = new ZNRecordSerializer();
    zkClient.setZkSerializer(zkSerializer);
    BaseDataAccessor<ZNRecord> baseDataAccessor = new ZkBaseDataAccessor<>(zkClient);
    HelixDataAccessor helixDataAccessor = new ZKHelixDataAccessor(zkCluster, baseDataAccessor);
    Builder keyBuilder = helixDataAccessor.keyBuilder();
    PropertyKey idealStateKey = keyBuilder.idealStates(tableName);
    PropertyKey externalViewKey = keyBuilder.externalView(tableName);
    IdealState currentIdealState = helixDataAccessor.getProperty(idealStateKey);
    byte[] serializeIS = zkSerializer.serialize(currentIdealState.getRecord());
    String name = tableName + ".idealstate." + System.currentTimeMillis();
    File outputFile = new File("/tmp", name);

    try (FileOutputStream fileOutputStream = new FileOutputStream(outputFile)) {
      IOUtils.write(serializeIS, fileOutputStream);
    } catch (IOException e) {
      LOG.error("Exception in delete overlapping segments", e);
      return updateSuccessful;
    }
    LOG.info("Saved current idealstate to {}", outputFile);
    IdealState newIdealState;
    do {
      newIdealState = computeNewIdealStateAfterDeletingOverlappingSegments(helixDataAccessor, idealStateKey);
      LOG.info("Updating IdealState");
      updateSuccessful = helixDataAccessor.getBaseDataAccessor().set(idealStateKey.getPath(), newIdealState.getRecord(), newIdealState.getRecord().getVersion(), AccessOption.PERSISTENT);
      if (updateSuccessful) {
        int numSegmentsDeleted = currentIdealState.getPartitionSet().size() - newIdealState.getPartitionSet().size();
        LOG.info("Successfully updated IdealState: Removed segments: {}", (numSegmentsDeleted));
      }
    } while (!updateSuccessful);

    try {
      while (true) {
        Thread.sleep(10000);
        ExternalView externalView = helixDataAccessor.getProperty(externalViewKey);
        IdealState idealState = helixDataAccessor.getProperty(idealStateKey);
        Set<String> evPartitionSet = externalView.getPartitionSet();
        Set<String> isPartitionSet = idealState.getPartitionSet();
        if (evPartitionSet.equals(isPartitionSet)) {
          LOG.info("Table {} has reached stable state. i.e segments in external view match idealstates", tableName);
          break;
        }
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    return updateSuccessful;
  }
}
