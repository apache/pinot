/*******************************************************************************
 * © [2013] LinkedIn Corp. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.linkedin.pinot.server.zookeeper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;


/**
 * Segment level interaction with zookeeper.
 * 
 * @author xiafu
 *
 */
public class SegmentZkUtils {
  private static Logger logger = LoggerFactory.getLogger(SegmentZkUtils.class);

  public static void addToActiveSegments(ZkClient zkClient, String resourceName, int partition, String segmentId) {
    String path = getActiveSegmentsPath(resourceName, partition, segmentId);
    if (!zkClient.exists(path)) {
      zkClient.createPersistent(path, true);
    }
  }

  public static Properties getMetadata(ZkClient zkClient, String resourceName, String segmentId) throws IOException {
    String segmentInfoPath = getSegmentInfoPath(resourceName, segmentId);
    String metadataPath = segmentInfoPath + "/metadata";
    if (!zkClient.exists(metadataPath)) {
      return null;
    }
    byte[] data = zkClient.readData(metadataPath);
    Properties properties = new Properties();
    properties.load(new ByteArrayInputStream(data));
    return properties;
  }

  public static boolean removeFromActiveSegments(ZkClient zkClient, String resourceName, int partition, String segmentId) {
    String path = getActiveSegmentsPath(resourceName, partition, segmentId);
    String refreshPath = getRefreshMarkerPath(resourceName, partition, segmentId);
    if (zkClient.exists(refreshPath)) {
      zkClient.deleteRecursive(refreshPath);
    }
    if (zkClient.exists(path)) {
      zkClient.deleteRecursive(path);
      return true;
    } else {
      return false;
    }
  }

  public static int removeFromActiveSegments(ZkClient zkClient, String resourceName, int partition, int maxPartition,
      String segmentId) {
    if (removeFromActiveSegments(zkClient, resourceName, partition, segmentId)) {
      return partition;
    }
    for (int i = 0; i <= maxPartition; ++i) {
      if (partition == i) {
        continue;
      }
      if (removeFromActiveSegments(zkClient, resourceName, partition, segmentId)) {
        return i;
      }
    }
    return -1;
  }

  public static String getActiveSegmentsPathForPartition(String resourceName, int partition) {
    return getActiveSegmentsPath(resourceName) + "/" + partition;
  }

  public static String getActiveSegmentsPath(String resourceName) {
    return getZkRoot() + "/" + resourceName + "/activeSegments";
  }

  public static String getZkRoot() {
    return "/pinot";
  }

  public static String getMasterZkPath(String resourceName) {
    return SegmentZkUtils.getZkRoot() + "/" + resourceName + "/masters";
  }

  public static String getRefreshMarkerPath(String resourceName) {
    return SegmentZkUtils.getZkRoot() + "/" + resourceName + "/refreshMarkers";
  }

  public static String getRefreshMarkerPath(String resourceName, int partition) {
    return getRefreshMarkerPath(resourceName) + "/" + partition;
  }

  public static String getRefreshMarkerPath(String resourceName, int partition, String segmentId) {
    return getRefreshMarkerPath(resourceName) + "/" + partition + "/" + segmentId;
  }

  public static int getPartitionIdFromActiveSegments(ZkClient zkClient, String resourceName, int partitionId,
      String segmentId) {
    String activeSegmentsPath = getActiveSegmentsPath(resourceName, partitionId, segmentId);
    if (zkClient.exists(activeSegmentsPath)) {
      return partitionId;
    }
    // Go through all the partitions
    List<String> partitionsList = zkClient.getChildren(getActiveSegmentsPath(resourceName));
    for (String partitionString : partitionsList) {
      int partition = Integer.parseInt(partitionString);
      if (partition == partitionId) {
        continue;
      }
      activeSegmentsPath = getActiveSegmentsPath(resourceName, partition, segmentId);
      if (zkClient.exists(activeSegmentsPath)) {
        return partition;
      }
    }
    // SegmentId only in segmentInfo, not in activeSegments
    return -1;
  }

  public static String getActiveSegmentsPath(String resourceName, int partition, String segmentId) {
    return getActiveSegmentsPath(resourceName) + "/" + partition + "/" + segmentId;
  }

  public static String getSegmentInfoPath(String resourceName, String segmentId) {
    return getZkRoot() + "/" + resourceName + "/segmentInfo/" + segmentId;
  }

  public static String getSegmentInfoPath(String resourceName) {
    return getZkRoot() + "/" + resourceName + "/segmentInfo";
  }

  public static void registerAsActiveSegment(ZkClient zkClient, String resourceName, int partition, String segmentId) {
    String activeSegmentsPath = getActiveSegmentsPath(resourceName, partition, segmentId);
    if (!zkClient.exists(activeSegmentsPath)) {
      zkClient.createPersistent(activeSegmentsPath, true);
    }
  }

  @SuppressWarnings("unchecked")
  public static List<String> getresourceNames(ZkClient zkClient) {

    if (!zkClient.exists(getZkRoot())) {
      return Collections.EMPTY_LIST;
    }
    return zkClient.getChildren(getZkRoot());
  }

  public static boolean isSegmentInfoReady(ZkClient zkClient, String resourceName, String segmentId) {
    try {
      String zkPath = getSegmentInfoPath(resourceName, segmentId);
      if (!zkClient.exists(zkPath)) {
        return false;
      }
      String readyPath = zkPath + "/readyFlag";
      if (!zkClient.exists(readyPath)) {
        return false;
      }
    } catch (Exception ex) {
      logger.error(ex.toString());
    }
    return true;
  }

  public static void moveSegment(ZkClient zkClient, String resourceName, String segmentId, int oldPartition,
      int newPartition) {
    SegmentInfo retrievedFromZookeeper = SegmentInfo.retrieveFromZookeeper(zkClient, resourceName, segmentId);
    Assert.notNull(retrievedFromZookeeper);
    SegmentZkUtils.removeFromActiveSegments(zkClient, resourceName, oldPartition, segmentId);
    SegmentZkUtils.addToActiveSegments(zkClient, resourceName, newPartition, segmentId);

  }
}
