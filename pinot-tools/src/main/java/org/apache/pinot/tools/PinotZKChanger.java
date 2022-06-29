/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class provides utilities that change zookeeper state in a cluster
 */
public class PinotZKChanger {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotZKChanger.class);

  protected ZKHelixAdmin _helixAdmin;
  protected HelixManager _helixManager;
  protected String _clusterName;
  protected ZkHelixPropertyStore<ZNRecord> _propertyStore;

  public PinotZKChanger(String zkAddress, String clusterName) {
    _clusterName = clusterName;
    _helixAdmin = new ZKHelixAdmin(zkAddress);
    _helixManager = HelixManagerFactory
        .getZKHelixManager(clusterName, "PinotNumReplicaChanger", InstanceType.ADMINISTRATOR, zkAddress);
    try {
      _helixManager.connect();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    String path = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, clusterName);
    _propertyStore = new ZkHelixPropertyStore<>(zkAddress, serializer, path);
  }

  public ZKHelixAdmin getHelixAdmin() {
    return _helixAdmin;
  }

  /**
   * return true if IdealState = ExternalView
   * @return
   */
  public int isStable(String tableName) {
    IdealState idealState = _helixAdmin.getResourceIdealState(_clusterName, tableName);
    ExternalView externalView = _helixAdmin.getResourceExternalView(_clusterName, tableName);
    Map<String, Map<String, String>> mapFieldsIS = idealState.getRecord().getMapFields();
    Map<String, Map<String, String>> mapFieldsEV = externalView.getRecord().getMapFields();
    int numDiff = 0;
    for (String segment : mapFieldsIS.keySet()) {
      Map<String, String> mapIS = mapFieldsIS.get(segment);
      Map<String, String> mapEV = mapFieldsEV.get(segment);

      for (String server : mapIS.keySet()) {
        String state = mapIS.get(server);
        if (mapEV == null || mapEV.get(server) == null || !mapEV.get(server).equals(state)) {
          LOGGER.info(
              "Mismatch: segment " + segment + " server:" + server + " expected state:" + state + " actual state:" + (
                  (mapEV == null || mapEV.get(server) == null) ? "null" : mapEV.get(server)));
          numDiff = numDiff + 1;
        }
      }
    }
    return numDiff;
  }

  /**
   * Wait till state has stabilized {@link #isStable(String)}
   * @param resourceName
   * @throws InterruptedException
   */
  public void waitForStable(String resourceName)
      throws InterruptedException {
    int diff;
    Thread.sleep(3000);
    do {
      diff = isStable(resourceName);
      if (diff == 0) {
        break;
      } else {
        LOGGER.info(
            "Waiting for externalView to match idealstate for table:" + resourceName + " Num segments difference:"
                + diff);
        Thread.sleep(30000);
      }
    } while (diff > 0);
  }

  protected void printSegmentAssignment(Map<String, Map<String, String>> mapping)
      throws Exception {
    LOGGER.info(JsonUtils.objectToPrettyString(mapping));
    Map<String, List<String>> serverToSegmentMapping = new TreeMap<>();
    for (String segment : mapping.keySet()) {
      Map<String, String> serverToStateMap = mapping.get(segment);
      for (String server : serverToStateMap.keySet()) {
        if (!serverToSegmentMapping.containsKey(server)) {
          serverToSegmentMapping.put(server, new ArrayList<>());
        }
        serverToSegmentMapping.get(server).add(segment);
      }
    }
    DescriptiveStatistics stats = new DescriptiveStatistics();
    for (String server : serverToSegmentMapping.keySet()) {
      List<String> list = serverToSegmentMapping.get(server);
      LOGGER.info("server " + server + " has " + list.size() + " segments");
      stats.addValue(list.size());
    }
    LOGGER.info("Segment Distrbution stat");
    LOGGER.info(stats.toString());
  }

  public HelixManager getHelixManager() {
    return _helixManager;
  }
}
