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
import java.util.HashMap;
import java.util.List;
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
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MasterSlaveSMD;
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

import static org.apache.pinot.common.utils.CommonConstants.Helix.ENABLE_DELAY_REBALANCE;
import static org.apache.pinot.common.utils.CommonConstants.Helix.LEAD_CONTROLLER_RESOURCE_NAME;
import static org.apache.pinot.common.utils.CommonConstants.Helix.MIN_ACTIVE_REPLICAS;
import static org.apache.pinot.common.utils.CommonConstants.Helix.REBALANCE_DELAY_MS;


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
    final HelixAdmin admin = new ZKHelixAdmin(zkPath);
    Preconditions.checkState(admin.getClusters().contains(helixClusterName),
        String.format("Helix cluster: %s hasn't been set up", helixClusterName));

    // Add segment state model definition if needed
    addSegmentStateModelDefinitionIfNeeded(helixClusterName, admin, zkPath, isUpdateStateModel);

    // Add broker resource if needed
    createBrokerResourceIfNeeded(helixClusterName, admin, enableBatchMessageMode);

    // Add lead controller resource if needed
    createLeadControllerResourceIfNeeded(helixClusterName, admin, enableBatchMessageMode);

    // Init property store if needed
    initPropertyStoreIfNeeded(helixClusterName, zkPath);
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
      // Set min active replicas to 0 and rebalance delay to 5 minutes so that if any master goes offline, Helix controller waits at most 5 minutes and then re-calculate the participant assignment.
      // This delay is helpful when periodic tasks are running and we don't want them to be re-run too frequently.
      // Plus, if virtual id is applied to controller hosts, swapping hosts would be easy as new hosts can use the same virtual id and it takes least effort to change the configs.
      leadControllerIdealState.setMinActiveReplicas(MIN_ACTIVE_REPLICAS);
      leadControllerIdealState.setRebalanceDelay(REBALANCE_DELAY_MS);
      leadControllerIdealState.setDelayRebalanceEnabled(ENABLE_DELAY_REBALANCE);
      admin.setResourceIdealState(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME, leadControllerIdealState);

      // Explicitly disable this resource when creating this new resource.
      // When all the controllers are running the code with the logic to handle this resource, it can be enabled for backward compatibility.
      // In the next major release, we can enable this resource by default, so that all the controller logic can be separated.
      admin.enableResource(helixClusterName, LEAD_CONTROLLER_RESOURCE_NAME, false);

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
