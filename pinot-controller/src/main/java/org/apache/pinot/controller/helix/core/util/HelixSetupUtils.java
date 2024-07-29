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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.model.builder.FullAutoModeISBuilder;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixBrokerResourceOnlineOfflineStateModelGenerator;
import org.apache.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.*;


public class HelixSetupUtils {
  private HelixSetupUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(HelixSetupUtils.class);

  public static HelixManager setupHelixController(String helixClusterName, String zkPath, String instanceId) {
    setupHelixClusterIfNeeded(helixClusterName, zkPath);
    return HelixControllerMain
        .startHelixController(zkPath, helixClusterName, instanceId, HelixControllerMain.STANDALONE);
  }

  private static void setupHelixClusterIfNeeded(String helixClusterName, String zkPath) {
    HelixAdmin admin = null;
    try {
      admin = new ZKHelixAdmin.Builder().setZkAddress(zkPath).build();
      if (admin.getClusters().contains(helixClusterName)) {
        LOGGER.info("Helix cluster: {} already exists", helixClusterName);
      } else {
        LOGGER.info("Creating a new Helix cluster: {}", helixClusterName);
        admin.addCluster(helixClusterName, false);
        // Enable Auto-Join for the cluster
        HelixConfigScope configScope =
            new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(helixClusterName).build();
        Map<String, String> configMap = new HashMap<>();
        configMap.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, Boolean.toString(true));
        configMap.put(ENABLE_CASE_INSENSITIVE_KEY, Boolean.toString(DEFAULT_ENABLE_CASE_INSENSITIVE));
        configMap.put(DEFAULT_HYPERLOGLOG_LOG2M_KEY, Integer.toString(DEFAULT_HYPERLOGLOG_LOG2M));
        configMap.put(CommonConstants.Broker.CONFIG_OF_ENABLE_QUERY_LIMIT_OVERRIDE, Boolean.toString(false));
        admin.setConfig(configScope, configMap);
        LOGGER.info("New Helix cluster: {} created", helixClusterName);
      }
    } finally {
      if (admin != null) {
        admin.close();
      }
    }
  }

  public static void setupPinotCluster(String helixClusterName, String zkPath, boolean isUpdateStateModel,
      boolean enableBatchMessageMode, ControllerConf controllerConf) {
    ZkClient zkClient = null;
    int zkClientSessionConfig =
        controllerConf.getProperty(CommonConstants.Helix.ZkClient.ZK_CLIENT_SESSION_TIMEOUT_MS_CONFIG,
            CommonConstants.Helix.ZkClient.DEFAULT_SESSION_TIMEOUT_MS);
    int zkClientConnectionTimeoutMs =
        controllerConf.getProperty(CommonConstants.Helix.ZkClient.ZK_CLIENT_CONNECTION_TIMEOUT_MS_CONFIG,
            CommonConstants.Helix.ZkClient.DEFAULT_CONNECT_TIMEOUT_MS);
    try {
      zkClient = new ZkClient.Builder()
          .setZkServer(zkPath)
          .setSessionTimeout(zkClientSessionConfig)
          .setConnectionTimeout(zkClientConnectionTimeoutMs)
          .setZkSerializer(new ZNRecordSerializer())
          .build();
      zkClient.waitUntilConnected(zkClientConnectionTimeoutMs, TimeUnit.MILLISECONDS);
      HelixAdmin helixAdmin = new ZKHelixAdmin(zkClient);
      HelixDataAccessor helixDataAccessor =
          new ZKHelixDataAccessor(helixClusterName, new ZkBaseDataAccessor<>(zkClient));
      ConfigAccessor configAccessor = new ConfigAccessor(zkClient);

      Preconditions.checkState(helixAdmin.getClusters().contains(helixClusterName),
          String.format("Helix cluster: %s hasn't been set up", helixClusterName));

      // Add segment state model definition if needed
      addSegmentStateModelDefinitionIfNeeded(helixClusterName, helixAdmin, helixDataAccessor, isUpdateStateModel);

      // Add broker resource if needed
      createBrokerResourceIfNeeded(helixClusterName, helixAdmin, enableBatchMessageMode);

      // Add lead controller resource if needed
      createLeadControllerResourceIfNeeded(helixClusterName, helixAdmin, configAccessor, enableBatchMessageMode,
          controllerConf);
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
    // Add state model definition if needed
    String stateModel =
        PinotHelixBrokerResourceOnlineOfflineStateModelGenerator.PINOT_BROKER_RESOURCE_ONLINE_OFFLINE_STATE_MODEL;
    StateModelDefinition stateModelDef = helixAdmin.getStateModelDef(helixClusterName, stateModel);
    if (stateModelDef == null) {
      LOGGER.info("Adding state model: {}", stateModel);
      helixAdmin.addStateModelDef(helixClusterName, stateModel,
          PinotHelixBrokerResourceOnlineOfflineStateModelGenerator.generatePinotStateModelDefinition());
    }

    // Add broker resource if needed
    if (helixAdmin.getResourceIdealState(helixClusterName, BROKER_RESOURCE_INSTANCE) == null) {
      LOGGER.info("Adding resource: {}", BROKER_RESOURCE_INSTANCE);
      IdealState idealState = new CustomModeISBuilder(BROKER_RESOURCE_INSTANCE).setStateModel(stateModel).build();
      idealState.setBatchMessageMode(enableBatchMessageMode);
      helixAdmin.addResource(helixClusterName, BROKER_RESOURCE_INSTANCE, idealState);
    }
  }

  private static void createLeadControllerResourceIfNeeded(String helixClusterName, HelixAdmin helixAdmin,
      ConfigAccessor configAccessor, boolean enableBatchMessageMode, ControllerConf controllerConf) {
    IdealState currentIdealState = helixAdmin.getResourceIdealState(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME);
    if (currentIdealState == null) {
      LOGGER.info("Adding resource: {}", LEAD_CONTROLLER_RESOURCE_NAME);
      IdealState newIdealState = constructIdealState(enableBatchMessageMode, controllerConf);
      helixAdmin.addResource(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME, newIdealState);
    } else {
      enableAndUpdateLeadControllerResource(helixClusterName, helixAdmin, currentIdealState, enableBatchMessageMode,
          controllerConf);
    }

    // Create resource config for lead controller resource if it doesn't exist
    ResourceConfig resourceConfig = configAccessor.getResourceConfig(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME);
    if (resourceConfig == null) {
      resourceConfig = new ResourceConfig(LEAD_CONTROLLER_RESOURCE_NAME);
    }
    // Explicitly set RESOURCE_ENABLED to true, so that the lead controller resource is always enabled.
    // This is to help keep the behavior consistent in all clusters for better maintenance.
    // TODO: remove the logic of handling this config in both controller and server in the next official release.
    resourceConfig.putSimpleConfig(LEAD_CONTROLLER_RESOURCE_ENABLED_KEY, Boolean.TRUE.toString());
    configAccessor.setResourceConfig(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME, resourceConfig);
  }

  private static IdealState constructIdealState(boolean enableBatchMessageMode, ControllerConf controllerConf) {
    // FULL-AUTO Master-Slave state model with a rebalance strategy, auto-rebalance by default
    FullAutoModeISBuilder idealStateBuilder = new FullAutoModeISBuilder(LEAD_CONTROLLER_RESOURCE_NAME);
    idealStateBuilder.setStateModel(MasterSlaveSMD.name)
        .setRebalanceStrategy(controllerConf.getLeadControllerResourceRebalanceStrategy());
    // Initialize partitions and replicas
    idealStateBuilder.setNumPartitions(NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE);
    for (int i = 0; i < NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE; i++) {
      idealStateBuilder.add(LeadControllerUtils.generatePartitionName(i));
    }
    idealStateBuilder.setNumReplica(LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT);
    // The below config guarantees if active number of replicas is no less than minimum active replica, there will
    // not be partition movements happened.
    // Set min active replicas to 0 and rebalance delay to 5 minutes so that if any master goes offline, Helix
    // controller waits at most 5 minutes and then re-calculate the participant assignment.
    // This delay is helpful when periodic tasks are running and we don't want them to be re-run too frequently.
    // Plus, if virtual id is applied to controller hosts, swapping hosts would be easy as new hosts can use the
    // same virtual id and it takes least effort to change the configs.
    idealStateBuilder.setMinActiveReplica(MIN_ACTIVE_REPLICAS);
    idealStateBuilder.setRebalanceDelay(controllerConf.getLeadControllerResourceRebalanceDelayMs());
    idealStateBuilder.enableDelayRebalance();
    // Set instance group tag
    IdealState idealState = idealStateBuilder.build();
    idealState.setInstanceGroupTag(CONTROLLER_INSTANCE);
    // Set batch message mode
    idealState.setBatchMessageMode(enableBatchMessageMode);
    return idealState;
  }

  /**
   * If user defined properties for the lead controller have changed, update the resource.
   */
  private static void enableAndUpdateLeadControllerResource(String helixClusterName, HelixAdmin helixAdmin,
      IdealState idealState, boolean enableBatchMessageMode, ControllerConf controllerConf) {
    boolean needsUpdating = false;

    if (!idealState.isEnabled()) {
      LOGGER.info("Enabling resource: {}", LEAD_CONTROLLER_RESOURCE_NAME);
      // Enable lead controller resource and let resource config be the only switch for enabling logic of lead
      // controller resource.
      idealState.enable(true);
      needsUpdating = true;
    }
    if (idealState.getBatchMessageMode() != enableBatchMessageMode) {
      LOGGER.info("Updating batch message mode to: {} for resource: {}", enableBatchMessageMode,
          LEAD_CONTROLLER_RESOURCE_NAME);
      idealState.setBatchMessageMode(enableBatchMessageMode);
      needsUpdating = true;
    }
    if (!idealState.getRebalanceStrategy().equals(controllerConf.getLeadControllerResourceRebalanceStrategy())) {
      LOGGER.info("Updating rebalance strategy to: {} for resource: {}",
          controllerConf.getLeadControllerResourceRebalanceStrategy(), LEAD_CONTROLLER_RESOURCE_NAME);
      idealState.setRebalanceStrategy(controllerConf.getLeadControllerResourceRebalanceStrategy());
      needsUpdating = true;
    }
    if (idealState.getRebalanceDelay() != controllerConf.getLeadControllerResourceRebalanceDelayMs()) {
      LOGGER.info("Updating rebalance delay to: {} for resource: {}",
          controllerConf.getLeadControllerResourceRebalanceDelayMs(), LEAD_CONTROLLER_RESOURCE_NAME);
      idealState.setRebalanceDelay(controllerConf.getLeadControllerResourceRebalanceDelayMs());
      needsUpdating = true;
    }

    if (needsUpdating) {
      LOGGER.info("Updating ideal state for resource: {}", LEAD_CONTROLLER_RESOURCE_NAME);
      helixAdmin.updateIdealState(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME, idealState);
    }
  }
}
