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
package org.apache.pinot.controller.helix.core.util;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.manager.zk.client.SharedZkClientFactory;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.controller.helix.core.PinotHelixBrokerResourceOnlineOfflineStateModelGenerator;
import org.apache.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import org.apache.pinot.controller.helix.core.PinotTableIdealStateBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.common.utils.CommonConstants.Helix.*;


/**
 * HelixSetupUtils handles how to create or get a helixCluster in controller.
 *
 *
 */
public class HelixSetupUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixSetupUtils.class);

  public static HelixManager setup(String helixClusterName, String zkPath, String helixControllerInstanceId) {
    setupHelixClusterIfNeeded(helixClusterName, zkPath);
    return startHelixControllerInStandaloneMode(helixClusterName, zkPath, helixControllerInstanceId);
  }

  /**
   * Set up a brand new Helix cluster if it doesn't exist.
   */
  private static void setupHelixClusterIfNeeded(String helixClusterName, String zkPath) {
    HelixAdmin admin = new ZKHelixAdmin(zkPath);
    if (admin.getClusters().contains(helixClusterName)) {
      LOGGER.info("Helix cluster: {} already exists", helixClusterName);
    } else {
      LOGGER.info("Creating a new Helix cluster: {}", helixClusterName);
      admin.addCluster(helixClusterName, false);
      // Enable Auto-Join for the cluster
      HelixConfigScope configScope =
          new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(helixClusterName).build();
      admin.setConfig(configScope, Collections.singletonMap(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, "true"));
      LOGGER.info("New Helix cluster: {} created", helixClusterName);
    }
  }

  private static HelixManager startHelixControllerInStandaloneMode(String helixClusterName, String zkUrl,
      String pinotControllerInstanceId) {
    LOGGER.info("Starting Helix Standalone Controller ... ");
    return HelixControllerMain
        .startHelixController(zkUrl, helixClusterName, pinotControllerInstanceId, HelixControllerMain.STANDALONE);
  }

  /**
   * Customizes existing Helix cluster to run Pinot components.
   */
  public static void setupPinotCluster(String helixClusterName, String zkPath, boolean isUpdateStateModel,
      boolean enableBatchMessageMode) {
    HelixZkClient zkClient = null;
    try {
      zkClient = SharedZkClientFactory.getInstance().buildZkClient(new HelixZkClient.ZkConnectionConfig(zkPath),
          new HelixZkClient.ZkClientConfig().setZkSerializer(new ZNRecordSerializer())
              .setConnectInitTimeout(TimeUnit.SECONDS.toMillis(ZkClient.DEFAULT_CONNECT_TIMEOUT_SEC)));
      zkClient.waitUntilConnected(ZkClient.DEFAULT_CONNECT_TIMEOUT_SEC, TimeUnit.SECONDS);
      HelixAdmin helixAdmin = new ZKHelixAdmin(zkClient);
      HelixDataAccessor helixDataAccessor =
          new ZKHelixDataAccessor(helixClusterName, new ZkBaseDataAccessor<>(zkClient));

      Preconditions.checkState(helixAdmin.getClusters().contains(helixClusterName),
          String.format("Helix cluster: %s hasn't been set up", helixClusterName));

      // Add segment state model definition if needed
      addSegmentStateModelDefinitionIfNeeded(helixClusterName, helixAdmin, helixDataAccessor, isUpdateStateModel);

      // Add broker resource if needed
      createBrokerResourceIfNeeded(helixClusterName, helixAdmin, enableBatchMessageMode);

      // Add lead controller resource if needed
      createLeadControllerResourceIfNeeded(helixClusterName, helixAdmin, enableBatchMessageMode);
    } finally {
      if (zkClient != null) {
        zkClient.close();
      }
    }
  }

  private static void addSegmentStateModelDefinitionIfNeeded(String helixClusterName, HelixAdmin helixAdmin,
      HelixDataAccessor helixDataAccessor, boolean isUpdateStateModel) {
    String segmentStateModelName =
        PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL;
    StateModelDefinition stateModelDefinition = helixAdmin.getStateModelDef(helixClusterName, segmentStateModelName);
    if (stateModelDefinition == null || isUpdateStateModel) {
      if (stateModelDefinition == null) {
        LOGGER.info("Adding state model: {} with CONSUMING state", segmentStateModelName);
      } else {
        LOGGER.info("Updating state model: {} to contain CONSUMING state", segmentStateModelName);
      }
      helixDataAccessor
          .createStateModelDef(PinotHelixSegmentOnlineOfflineStateModelGenerator.generatePinotStateModelDefinition());
    }
  }

  private static void createBrokerResourceIfNeeded(String helixClusterName, HelixAdmin helixAdmin,
      boolean enableBatchMessageMode) {
    // Add broker resource online offline state model definition if needed
    StateModelDefinition brokerResourceStateModelDefinition = helixAdmin.getStateModelDef(helixClusterName,
        PinotHelixBrokerResourceOnlineOfflineStateModelGenerator.PINOT_BROKER_RESOURCE_ONLINE_OFFLINE_STATE_MODEL);
    if (brokerResourceStateModelDefinition == null) {
      LOGGER.info("Adding state model definition named : {} generated using : {}",
          PinotHelixBrokerResourceOnlineOfflineStateModelGenerator.PINOT_BROKER_RESOURCE_ONLINE_OFFLINE_STATE_MODEL,
          PinotHelixBrokerResourceOnlineOfflineStateModelGenerator.class.toString());
      helixAdmin.addStateModelDef(helixClusterName,
          PinotHelixBrokerResourceOnlineOfflineStateModelGenerator.PINOT_BROKER_RESOURCE_ONLINE_OFFLINE_STATE_MODEL,
          PinotHelixBrokerResourceOnlineOfflineStateModelGenerator.generatePinotStateModelDefinition());
    }

    // Create broker resource if needed.
    if (helixAdmin.getResourceIdealState(helixClusterName, BROKER_RESOURCE_INSTANCE) == null) {
      LOGGER.info("Adding empty ideal state for Broker!");
      IdealState emptyIdealStateForBrokerResource = PinotTableIdealStateBuilder
          .buildEmptyIdealStateForBrokerResource(helixAdmin, helixClusterName, enableBatchMessageMode);
      helixAdmin.setResourceIdealState(helixClusterName, BROKER_RESOURCE_INSTANCE, emptyIdealStateForBrokerResource);
    }
  }

  private static void createLeadControllerResourceIfNeeded(String helixClusterName, HelixAdmin helixAdmin,
      boolean enableBatchMessageMode) {
    if (helixAdmin.getResourceIdealState(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME) == null) {
      LOGGER.info("Cluster {} doesn't contain {}. Creating one.", helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME);

      // FULL-AUTO Master-Slave state model with CrushED rebalance strategy.
      IdealState leadControllerResourceIdealState = new IdealState(LEAD_CONTROLLER_RESOURCE_NAME);
      leadControllerResourceIdealState.setNumPartitions(NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE);
      leadControllerResourceIdealState.setReplicas(Integer.toString(0));
      leadControllerResourceIdealState.setStateModelDefRef(MasterSlaveSMD.name);
      leadControllerResourceIdealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      leadControllerResourceIdealState.setRebalanceStrategy(CrushEdRebalanceStrategy.class.getName());
      // Set instance group tag for lead controller resource.
      leadControllerResourceIdealState.setInstanceGroupTag(CONTROLLER_INSTANCE_TYPE);
      leadControllerResourceIdealState.setBatchMessageMode(enableBatchMessageMode);
      // The below config guarantees if active number of replicas is no less than minimum active replica, there will not be partition movements happened.
      // Set min active replicas to 0 and rebalance delay to 5 minutes so that if any master goes offline, Helix controller waits at most 5 minutes and then re-calculate the participant assignment.
      // This delay is helpful when periodic tasks are running and we don't want them to be re-run too frequently.
      // Plus, if virtual id is applied to controller hosts, swapping hosts would be easy as new hosts can use the same virtual id and it takes least effort to change the configs.
      leadControllerResourceIdealState.setMinActiveReplicas(MIN_ACTIVE_REPLICAS);
      leadControllerResourceIdealState.setRebalanceDelay(REBALANCE_DELAY_MS);
      leadControllerResourceIdealState.setDelayRebalanceEnabled(ENABLE_DELAY_REBALANCE);
      // Explicitly disable this resource when creating this new resource.
      // When all the controllers are running the code with the logic to handle this resource, it can be enabled for backward compatibility.
      // In the next major release, we can enable this resource by default, so that all the controller logic can be separated.
      // To enable the resource:
      // helixAdmin.enableResource(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME, true);
      // helixAdmin.rebalance(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME, LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT);
      leadControllerResourceIdealState.enable(false);

      helixAdmin.addResource(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME, leadControllerResourceIdealState);
    }
  }
}
