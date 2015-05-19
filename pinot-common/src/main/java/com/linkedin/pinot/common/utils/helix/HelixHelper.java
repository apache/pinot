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
package com.linkedin.pinot.common.utils.helix;

import com.google.common.base.Function;
import com.linkedin.pinot.common.utils.EqualityUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.metadata.ZKMetadataProvider;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metadata.resource.OfflineDataResourceZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.StringUtil;


public class HelixHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixHelper.class);

  public static final String BROKER_RESOURCE = CommonConstants.Helix.BROKER_RESOURCE_INSTANCE;

  /**
   * Updates the ideal state, retrying if necessary in case of concurrent updates to the ideal state.
   *
   * @param helixManager The HelixManager used to interact with the Helix cluster
   * @param resourceName The resource for which to update the ideal state
   * @param updater A function that returns an updated ideal state given an input ideal state
   */
  public static void updateIdealState(HelixManager helixManager, String resourceName, Function<IdealState, IdealState>
      updater) {
    HelixDataAccessor dataAccessor = helixManager.getHelixDataAccessor();
    PropertyKey propertyKey = dataAccessor.keyBuilder().idealStates(resourceName);

    while (true) {
      // Create an updated version of the ideal state
      IdealState idealState = dataAccessor.getProperty(propertyKey);
      IdealState updatedIdealState;
      try {
        updatedIdealState = updater.apply((IdealState) dataAccessor.getProperty(propertyKey));
      } catch (Exception e) {
        LOGGER.error("Caught exception while updating ideal state", e);
        return;
      }

      // If there are changes to apply, apply them
      if (!EqualityUtils.isEqual(idealState, updatedIdealState) && updatedIdealState != null) {
        // Break out if update is successful
        if (dataAccessor.updateProperty(propertyKey, updatedIdealState)) {
          return;
        } else {
          LOGGER.warn("Failed to update ideal state for resource {}, retrying.", resourceName);
        }
      } else {
        LOGGER.warn("Idempotent or null ideal state update for resource {}, skipping update.", resourceName);
        return;
      }
    }
  }

  public static List<String> getAllInstances(HelixAdmin admin, String clusterName) {
    return admin.getInstancesInCluster(clusterName);
  }

  public static Set<String> getAllInstancesForResource(IdealState state) {
    final Set<String> instances = new HashSet<String>();

    for (final String partition : state.getPartitionSet()) {
      for (final String instance : state.getInstanceSet(partition)) {
        instances.add(instance);
      }
    }
    return instances;
  }

  public static void toggleInstance(String instanceName, String clusterName, HelixAdmin admin, boolean toggle) {
    admin.enableInstance(clusterName, instanceName, toggle);
  }

  public static void toggleInstancesWithPinotInstanceList(List<String> instances, String clusterName, HelixAdmin admin,
      boolean toggle) {
    for (final String instance : instances) {
      toggleInstance(instance, clusterName, admin, toggle);
    }
  }

  public static void toggleInstancesWithInstanceNameSet(Set<String> instances, String clusterName, HelixAdmin admin,
      boolean toggle) {
    for (final String instanceName : instances) {
      toggleInstance(instanceName, clusterName, admin, toggle);
    }
  }

  public static Map<String, String> getInstanceConfigsMapFor(String instanceName, String clusterName, HelixAdmin admin) {
    final HelixConfigScope scope = getInstanceScopefor(clusterName, instanceName);
    final List<String> keys = admin.getConfigKeys(scope);
    return admin.getConfig(scope, keys);
  }

  public static HelixConfigScope getInstanceScopefor(String clusterName, String instanceName) {
    return new HelixConfigScopeBuilder(ConfigScopeProperty.PARTICIPANT, clusterName).forParticipant(instanceName)
        .build();
  }

  public static HelixConfigScope getResourceScopeFor(String clusterName, String resourceName) {
    return new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE, clusterName).forResource(resourceName).build();
  }

  public static Map<String, String> getResourceConfigsFor(String clusterName, String resourceName, HelixAdmin admin) {
    final HelixConfigScope scope = getResourceScopeFor(clusterName, resourceName);
    final List<String> keys = admin.getConfigKeys(scope);
    return admin.getConfig(scope, keys);
  }

  public static void updateResourceConfigsFor(Map<String, String> newConfigs, String resourceName, String clusterName,
      HelixAdmin admin) {
    final HelixConfigScope scope = getResourceScopeFor(clusterName, resourceName);
    admin.setConfig(scope, newConfigs);
  }

  public static void deleteResourcePropertyFromHelix(HelixAdmin admin, String clusterName, String resourceName,
      String configKey) {
    final List<String> keys = new ArrayList<String>();
    keys.add(configKey);

    final HelixConfigScope scope = getResourceScopeFor(clusterName, resourceName);
    admin.removeConfig(scope, keys);
  }

  public static IdealState getResourceIdealState(HelixManager manager, String resourceName) {
    final HelixDataAccessor accessor = manager.getHelixDataAccessor();
    final Builder builder = accessor.keyBuilder();
    return accessor.getProperty(builder.idealStates(resourceName));
  }

  public static ExternalView getExternalViewForResouce(HelixAdmin admin, String clusterName, String resourceName) {
    return admin.getResourceExternalView(clusterName, resourceName);
  }

  public static Map<String, String> getBrokerResourceConfig(HelixAdmin admin, String clusterName) {
    return HelixHelper.getResourceConfigsFor(clusterName, BROKER_RESOURCE, admin);
  }

  public static void updateBrokerConfig(Map<String, String> brokerResourceConfig, HelixAdmin admin, String clusterName) {
    HelixHelper.updateResourceConfigsFor(brokerResourceConfig, BROKER_RESOURCE, clusterName, admin);
  }

  public static IdealState getBrokerIdealStates(HelixAdmin admin, String clusterName) {
    return admin.getResourceIdealState(clusterName, BROKER_RESOURCE);
  }

  public static void deleteResourceFromBrokerResource(HelixAdmin helixAdmin, String helixClusterName, String resourceTag) {
    LOGGER.info("Trying to mark instance to dropped state");
    IdealState brokerIdealState = helixAdmin.getResourceIdealState(helixClusterName,
        CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    if (brokerIdealState.getPartitionSet().contains(resourceTag)) {
      Map<String, String> instanceStateMap = brokerIdealState.getInstanceStateMap(resourceTag);
      for (String instance : instanceStateMap.keySet()) {
        brokerIdealState.setPartitionState(resourceTag, instance, "DROPPED");
      }
      helixAdmin.setResourceIdealState(helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, brokerIdealState);
    }
    LOGGER.info("Trying to remove resource from idealstats");
    if (brokerIdealState.getPartitionSet().contains(resourceTag)) {
      brokerIdealState.getPartitionSet().remove(resourceTag);
      helixAdmin.setResourceIdealState(helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, brokerIdealState);
    }
  }

  public static void main(String[] args) {
    ZkClient zkClient =
        new ZkClient(StringUtil.join("/", StringUtils.chomp("zk-lva1-pinot.corp.linkedin.com:12913", "/")),
            ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
    String propertyStorePath = PropertyPathConfig.getPath(PropertyType.PROPERTYSTORE, "mpSprintDemoCluster");
    ZkHelixPropertyStore<ZNRecord> propertyStore =
        new ZkHelixPropertyStore<ZNRecord>(new ZkBaseDataAccessor<ZNRecord>(zkClient), propertyStorePath, null);

    OfflineDataResourceZKMetadata offlineDataResourceZKMetadata = ZKMetadataProvider.getOfflineResourceZKMetadata(propertyStore, "xlntBeta");

    offlineDataResourceZKMetadata.setResourceName("testXlnt");
    offlineDataResourceZKMetadata.setBrokerTag("testXlnt1");
    ZKMetadataProvider.setOfflineResourceZKMetadata(propertyStore, offlineDataResourceZKMetadata);

    InstanceZKMetadata instanceZKMetadata = ZKMetadataProvider.getInstanceZKMetadata(propertyStore, "Server_lva1-app0120.corp.linkedin.com_8001");
    instanceZKMetadata.setGroupId("testResource0", "testGroup0");
    instanceZKMetadata.setPartition("testResource0", "testPart0");
    ZKMetadataProvider.setInstanceZKMetadata(propertyStore, instanceZKMetadata);
    System.out.println(instanceZKMetadata);

    InstanceZKMetadata instanceZKMetadata2 = new InstanceZKMetadata();
    instanceZKMetadata2.setInstanceName("lva1-app0120.corp.linkedin.com");
    instanceZKMetadata2.setInstanceType("Server");
    instanceZKMetadata2.setInstancePort(8001);

  }
}

// DataUpdater<ZNRecord> updater = new DataUpdater<ZNRecord>()
// {
// private ZNRecord newIdeal;
//
// @Override
// public ZNRecord update(ZNRecord currentData)
// {
// return newIdeal;
// }
// };
// //List<DataUpdater<ZNRecord>> updaters = new ArrayList<DataUpdater<ZNRecord>>(updater);
// //List<String> paths = new ArrayList<String>(key.getPath());
// //accessor.updateChildren(paths, updaters, AccessOption.PERSISTENT);
// //return accessor.updateProperty(key, state);

