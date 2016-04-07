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

package com.linkedin.pinot.controller.helix;

import com.linkedin.pinot.common.metrics.ControllerGauge;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.utils.CommonConstants;
import java.util.List;
import java.util.Map;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class updates the gauges in the controller on every external view change
 */
public class ControllerExternalViewChangeListener implements ExternalViewChangeListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalViewChangeListener.class);
  private final ControllerMetrics _controllerMetrics;

  public ControllerExternalViewChangeListener(ControllerMetrics controllerMetrics) {
    _controllerMetrics = controllerMetrics;
  }

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
    long totBelowReplicationThreshold = 0;
    LOGGER.info("Got an external view change, size of list {} ", externalViewList.size());
    for (ExternalView externalView : externalViewList) {
      long numBelowReplicationThreshold = 0;
      // Ignore brokerResource
      if (externalView.getId().equalsIgnoreCase(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)) {
        continue;
      }
      String tableName = externalView.getResourceName();
      LOGGER.debug("Got external view change for table {}", tableName);
      for (String partitionName : externalView.getPartitionSet()) {
        long nReplicas = 0;
        LOGGER.debug("State for partition {} is {}", partitionName, externalView.getStateMap(partitionName));
        for (Map.Entry<String, String> serverAndState : externalView.getStateMap(partitionName).entrySet()) {
          // Count number of online replicas
          if (serverAndState.getValue().equals("ONLINE")) {
            nReplicas++;
          }
        }
        if (nReplicas <= 1){
          // Not enough replicas
          LOGGER.warn("Partition {} of table {} only has {} replicas", partitionName, tableName, nReplicas);
          numBelowReplicationThreshold++;
        }
      }
      _controllerMetrics.setValueOfTableGauge(externalView.getId(), ControllerGauge.NUM_BELOW_REPLICATION_THRESHOLD,
          numBelowReplicationThreshold);
      totBelowReplicationThreshold += numBelowReplicationThreshold;
    }
    LOGGER.info("Got an external view change, size of list {}, {} segments below replication threshold ",
        externalViewList.size(), totBelowReplicationThreshold);
  }
}
