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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.messaging.handling.HelixTaskExecutor;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.helix.core.PinotHelixBrokerResourceOnlineOfflineStateModelGenerator;
import org.apache.pinot.controller.helix.core.PinotHelixSegmentOnlineOfflineStateModelGenerator;
import org.apache.pinot.controller.helix.core.PinotTableIdealStateBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.common.utils.CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME;


/**
 * HelixSetupUtils handles how to create or get a helixCluster in controller.
 *
 *
 */
public class HelixSetupUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixSetupUtils.class);

  public static synchronized HelixManager setup(String helixClusterName, String zkPath,
      String helixControllerInstanceId) {
    setupHelixCluster(helixClusterName, zkPath);

    return startHelixControllerInStandadloneMode(helixClusterName, zkPath, helixControllerInstanceId);
  }

  /**
   * Set up a brand new Helix cluster if it doesn't exist.
   */
  public static void setupHelixCluster(String helixClusterName, String zkPath) {
    final HelixAdmin admin = new ZKHelixAdmin(zkPath);
    if (admin.getClusters().contains(helixClusterName)) {
      LOGGER.info("Helix cluster: {} already exists", helixClusterName);
      return;
    }
    LOGGER.info("Creating a new Helix cluster: {}", helixClusterName);
    admin.addCluster(helixClusterName, false);
    LOGGER.info("New Cluster: {} created.", helixClusterName);
  }

  private static HelixManager startHelixControllerInStandadloneMode(String helixClusterName, String zkUrl,
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
    final HelixAdmin admin = new ZKHelixAdmin(zkPath);
    Preconditions.checkState(admin.getClusters().contains(helixClusterName),
        String.format("Helix cluster: %s hasn't been set up", helixClusterName));

    // Ensure auto join.
    ensureAutoJoin(helixClusterName, admin);

    // Add segment state model definition if needed
    addSegmentStateModelDefinitionIfNeeded(helixClusterName, admin, zkPath, isUpdateStateModel);

    // Add broker resource if needed
    createBrokerResourceIfNeeded(helixClusterName, admin, enableBatchMessageMode);

    // Add lead controller resource if needed
    createLeadControllerResourceIfNeeded(helixClusterName, admin, enableBatchMessageMode);

    // Init property store if needed
    initPropertyStoreIfNeeded(helixClusterName, zkPath);
  }

  private static void ensureAutoJoin(String helixClusterName, HelixAdmin admin) {
    final HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(helixClusterName).build();
    String stateTransitionMaxThreads = MessageType.STATE_TRANSITION + "." + HelixTaskExecutor.MAX_THREADS;
    List<String> keys = new ArrayList<>();
    keys.add(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN);
    keys.add(stateTransitionMaxThreads);
    Map<String, String> configs = admin.getConfig(scope, keys);
    if (!Boolean.TRUE.toString().equals(configs.get(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN))) {
      configs.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, Boolean.TRUE.toString());
    }
    if (!Integer.toString(1).equals(configs.get(stateTransitionMaxThreads))) {
      configs.put(stateTransitionMaxThreads, String.valueOf(1));
    }
    admin.setConfig(scope, configs);
  }

  private static void addSegmentStateModelDefinitionIfNeeded(String helixClusterName, HelixAdmin admin, String zkPath,
      boolean isUpdateStateModel) {
    final String segmentStateModelName =
        PinotHelixSegmentOnlineOfflineStateModelGenerator.PINOT_SEGMENT_ONLINE_OFFLINE_STATE_MODEL;
    StateModelDefinition stateModelDefinition = admin.getStateModelDef(helixClusterName, segmentStateModelName);
    if (stateModelDefinition == null) {
      LOGGER.info("Adding state model {} (with CONSUMED state) generated using {}", segmentStateModelName,
          PinotHelixSegmentOnlineOfflineStateModelGenerator.class.toString());
      admin.addStateModelDef(helixClusterName, segmentStateModelName,
          PinotHelixSegmentOnlineOfflineStateModelGenerator.generatePinotStateModelDefinition());
    } else if (isUpdateStateModel) {
      final StateModelDefinition curStateModelDef = admin.getStateModelDef(helixClusterName, segmentStateModelName);
      List<String> states = curStateModelDef.getStatesPriorityList();
      if (states.contains(PinotHelixSegmentOnlineOfflineStateModelGenerator.CONSUMING_STATE)) {
        LOGGER.info("State model {} already updated to contain CONSUMING state", segmentStateModelName);
      } else {
        LOGGER.info("Updating {} to add states for low level consumers", segmentStateModelName);
        StateModelDefinition newStateModelDef =
            PinotHelixSegmentOnlineOfflineStateModelGenerator.generatePinotStateModelDefinition();
        ZkClient zkClient = new ZkClient(zkPath);
        zkClient.waitUntilConnected(CommonConstants.Helix.ZkClient.DEFAULT_CONNECT_TIMEOUT_SEC, TimeUnit.SECONDS);
        zkClient.setZkSerializer(new ZNRecordSerializer());
        HelixDataAccessor accessor = new ZKHelixDataAccessor(helixClusterName, new ZkBaseDataAccessor<>(zkClient));
        PropertyKey.Builder keyBuilder = accessor.keyBuilder();
        accessor.setProperty(keyBuilder.stateModelDef(segmentStateModelName), newStateModelDef);
        LOGGER.info("Completed updating state model {}", segmentStateModelName);
        zkClient.close();
      }
    }
  }

  private static void createBrokerResourceIfNeeded(String helixClusterName, HelixAdmin admin,
      boolean enableBatchMessageMode) {
    // Add broker resource online offline state model definition if needed
    StateModelDefinition brokerResourceStateModelDefinition = admin.getStateModelDef(helixClusterName,
        PinotHelixBrokerResourceOnlineOfflineStateModelGenerator.PINOT_BROKER_RESOURCE_ONLINE_OFFLINE_STATE_MODEL);
    if (brokerResourceStateModelDefinition == null) {
      LOGGER.info("Adding state model definition named : {} generated using : {}",
          PinotHelixBrokerResourceOnlineOfflineStateModelGenerator.PINOT_BROKER_RESOURCE_ONLINE_OFFLINE_STATE_MODEL,
          PinotHelixBrokerResourceOnlineOfflineStateModelGenerator.class.toString());
      admin.addStateModelDef(helixClusterName,
          PinotHelixBrokerResourceOnlineOfflineStateModelGenerator.PINOT_BROKER_RESOURCE_ONLINE_OFFLINE_STATE_MODEL,
          PinotHelixBrokerResourceOnlineOfflineStateModelGenerator.generatePinotStateModelDefinition());
    }

    // Create broker resource if needed.
    IdealState brokerResourceIdealState =
        admin.getResourceIdealState(helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    if (brokerResourceIdealState == null) {
      LOGGER.info("Adding empty ideal state for Broker!");
      HelixHelper
          .updateResourceConfigsFor(new HashMap<>(), CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, helixClusterName,
              admin);
      IdealState idealState = PinotTableIdealStateBuilder
          .buildEmptyIdealStateForBrokerResource(admin, helixClusterName, enableBatchMessageMode);
      admin.setResourceIdealState(helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, idealState);
    }
  }

  private static void createLeadControllerResourceIfNeeded(String helixClusterName, HelixAdmin admin,
      boolean enableBatchMessageMode) {
    StateModelDefinition masterSlaveStateModelDefinition =
        admin.getStateModelDef(helixClusterName, MasterSlaveSMD.name);
    if (masterSlaveStateModelDefinition == null) {
      LOGGER.info("Adding state model definition named : {} generated using : {}", MasterSlaveSMD.name,
          MasterSlaveSMD.class.toString());
      admin.addStateModelDef(helixClusterName, MasterSlaveSMD.name, MasterSlaveSMD.build());
    }

    IdealState leadControllerResourceIdealState =
        admin.getResourceIdealState(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME);
    if (leadControllerResourceIdealState == null) {
      LOGGER.info("Cluster {} doesn't contain {}. Creating one.", helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME);
      HelixHelper.updateResourceConfigsFor(new HashMap<>(), LEAD_CONTROLLER_RESOURCE_NAME, helixClusterName, admin);
      // FULL-AUTO Master-Slave state model with CrushED reBalance strategy.
      admin.addResource(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME,
          CommonConstants.Helix.NUMBER_OF_PARTITIONS_IN_LEAD_CONTROLLER_RESOURCE, MasterSlaveSMD.name,
          IdealState.RebalanceMode.FULL_AUTO.toString(), CrushEdRebalanceStrategy.class.getName());

      // Set instance group tag for lead controller resource.
      IdealState leadControllerIdealState =
          admin.getResourceIdealState(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME);
      leadControllerIdealState.setInstanceGroupTag(CommonConstants.Helix.CONTROLLER_INSTANCE_TYPE);
      leadControllerIdealState.setBatchMessageMode(enableBatchMessageMode);
      // The below config guarantees if active number of replicas is no less than minimum active replica, there will not be partition movements happened.
      // Set it to 0 so that whenever a master becomes unavailable, another available instance will immediately be chosen by Helix controller to become the new master.
      leadControllerIdealState.setMinActiveReplicas(0);
      admin.setResourceIdealState(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME, leadControllerIdealState);

      LOGGER.info("Re-balance lead controller resource with replicas: {}",
          CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT);
      // Set it to 1 so that there's only 1 instance (i.e. master) shown in every partitions.
      admin.rebalance(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME,
          CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_REPLICA_COUNT);
    }
  }

  private static void initPropertyStoreIfNeeded(String helixClusterName, String zkPath) {
    String propertyStorePath = PropertyPathBuilder.propertyStore(helixClusterName);
    ZkHelixPropertyStore<ZNRecord> propertyStore =
        new ZkHelixPropertyStore<>(zkPath, new ZNRecordSerializer(), propertyStorePath);
    if (!propertyStore.exists("/CONFIGS", AccessOption.PERSISTENT)) {
      propertyStore.create("/CONFIGS", new ZNRecord(""), AccessOption.PERSISTENT);
    }
    if (!propertyStore.exists("/CONFIGS/CLUSTER", AccessOption.PERSISTENT)) {
      propertyStore.create("/CONFIGS/CLUSTER", new ZNRecord(""), AccessOption.PERSISTENT);
    }
    if (!propertyStore.exists("/CONFIGS/TABLE", AccessOption.PERSISTENT)) {
      propertyStore.create("/CONFIGS/TABLE", new ZNRecord(""), AccessOption.PERSISTENT);
    }
    if (!propertyStore.exists("/CONFIGS/INSTANCE", AccessOption.PERSISTENT)) {
      propertyStore.create("/CONFIGS/INSTANCE", new ZNRecord(""), AccessOption.PERSISTENT);
    }
    if (!propertyStore.exists("/SCHEMAS", AccessOption.PERSISTENT)) {
      propertyStore.create("/SCHEMAS", new ZNRecord(""), AccessOption.PERSISTENT);
    }
    if (!propertyStore.exists("/SEGMENTS", AccessOption.PERSISTENT)) {
      propertyStore.create("/SEGMENTS", new ZNRecord(""), AccessOption.PERSISTENT);
    }
  }
}
