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
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.client.HelixZkClient;
import org.apache.helix.manager.zk.client.SharedZkClientFactory;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.model.builder.FullAutoModeISBuilder;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.controller.helix.core.PinotHelixBrokerResourceOnlineOfflineStateModelGenerator;
import org.apache.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.common.utils.CommonConstants.Helix.*;


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
      admin = new ZKHelixAdmin(zkPath);
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
        configMap.put(ENABLE_CASE_INSENSITIVE_KEY, Boolean.toString(false));
        configMap.put(DEFAULT_HYPERLOGLOG_LOG2M_KEY, Integer.toString(DEFAULT_HYPERLOGLOG_LOG2M));
        configMap.put(CommonConstants.Broker.CONFIG_OF_ENABLE_QUERY_LIMIT_OVERRIDE, Boolean.toString(false));
        configMap.put(RACK_AWARENESS_ENABLED_KEY, Boolean.toString(RACK_AWARENESS_ENABLED_DEFAULT_VALUE));
        configMap.put(RACK_AWARENESS_PROCESSOR_CLASS_KEY, RACK_AWARENESS_PROCESSOR_CLASS_DEFAULT_VALUE);
        configMap.put(RACK_AWARENESS_CONNECTION_MAX_RETRY_KEY,
            Integer.toString(RACK_AWARENESS_CONNECTION_MAX_RETRY_DEFAULT_VALUE));
        configMap.put(RACK_AWARENESS_CONNECTION_CONNECTION_TIME_OUT_KEY,
            Integer.toString(RACK_AWARENESS_CONNECTION_CONNECTION_TIME_OUT_DEFAULT_VALUE));
        configMap.put(RACK_AWARENESS_CONNECTION_REQUEST_TIME_OUT_KEY,
            Integer.toString(RACK_AWARENESS_CONNECTION_REQUEST_TIME_OUT_DEFAULT_VALUE));
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
      boolean enableBatchMessageMode, String leadControllerResourceRebalanceStrategy) {
    HelixZkClient zkClient = null;
    try {
      zkClient = SharedZkClientFactory.getInstance().buildZkClient(new HelixZkClient.ZkConnectionConfig(zkPath),
          new HelixZkClient.ZkClientConfig().setZkSerializer(new ZNRecordSerializer())
              .setConnectInitTimeout(TimeUnit.SECONDS.toMillis(ZkClient.DEFAULT_CONNECT_TIMEOUT_SEC)));
      zkClient.waitUntilConnected(ZkClient.DEFAULT_CONNECT_TIMEOUT_SEC, TimeUnit.SECONDS);
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
          leadControllerResourceRebalanceStrategy);
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
      ConfigAccessor configAccessor, boolean enableBatchMessageMode, String leadControllerResourceRebalanceStrategy) {
    IdealState idealState = helixAdmin.getResourceIdealState(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME);
    if (idealState == null) {
      LOGGER.info("Adding resource: {}", LEAD_CONTROLLER_RESOURCE_NAME);

      // FULL-AUTO Master-Slave state model with a rebalance strategy, auto-rebalance by default
      FullAutoModeISBuilder idealStateBuilder = new FullAutoModeISBuilder(LEAD_CONTROLLER_RESOURCE_NAME);
      idealStateBuilder.setStateModel(MasterSlaveSMD.name)
          .setRebalanceStrategy(leadControllerResourceRebalanceStrategy);
      // Initialize partitions and replicas
      idealStateBuilder.setNumPartitions(NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE);
      for (int i = 0; i < NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE; i++) {
        idealStateBuilder.add(LeadControllerUtils.generatePartitionName(i));
      }
      idealStateBuilder.setNumReplica(LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT);
      // The below config guarantees if active number of replicas is no less than minimum active replica, there will not be partition movements happened.
      // Set min active replicas to 0 and rebalance delay to 5 minutes so that if any master goes offline, Helix controller waits at most 5 minutes and then re-calculate the participant assignment.
      // This delay is helpful when periodic tasks are running and we don't want them to be re-run too frequently.
      // Plus, if virtual id is applied to controller hosts, swapping hosts would be easy as new hosts can use the same virtual id and it takes least effort to change the configs.
      idealStateBuilder.setMinActiveReplica(MIN_ACTIVE_REPLICAS);
      idealStateBuilder.setRebalanceDelay(REBALANCE_DELAY_MS);
      idealStateBuilder.enableDelayRebalance();
      // Set instance group tag
      idealState = idealStateBuilder.build();
      idealState.setInstanceGroupTag(CONTROLLER_INSTANCE);
      // Set batch message mode
      idealState.setBatchMessageMode(enableBatchMessageMode);
      // Explicitly disable this resource when creating this new resource.
      // When all the controllers are running the code with the logic to handle this resource, it can be enabled for backward compatibility.
      // In the next major release, we can enable this resource by default, so that all the controller logic can be separated.

      helixAdmin.addResource(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME, idealState);
    } else if (!idealState.isEnabled()) {
      // Enable lead controller resource and let resource config be the only switch for enabling logic of lead controller resource.
      idealState.enable(true);
      helixAdmin.updateIdealState(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME, idealState);
    }

    // Create resource config for lead controller resource if it doesn't exist
    ResourceConfig resourceConfig = configAccessor.getResourceConfig(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME);
    if (resourceConfig == null) {
      resourceConfig = new ResourceConfig(LEAD_CONTROLLER_RESOURCE_NAME);
    }
    // Set RESOURCE_ENABLED to false if it's absent in resource config
    if (resourceConfig.getSimpleConfig(LEAD_CONTROLLER_RESOURCE_ENABLED_KEY) == null) {
      resourceConfig.putSimpleConfig(LEAD_CONTROLLER_RESOURCE_ENABLED_KEY, Boolean.FALSE.toString());
    }
    configAccessor.setResourceConfig(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME, resourceConfig);
  }
}
