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
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.EqualityUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class HelixHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixHelper.class);

  private static final String ONLINE = "ONLINE";
  private static final String OFFLINE = "OFFLINE";
  private static final String DROPPED = "DROPPED";

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
    IdealState.RebalanceMode rebalanceMode = state.getRebalanceMode();
    state.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    for (final String partition : state.getPartitionSet()) {
      for (final String instance : state.getInstanceSet(partition)) {
        instances.add(instance);
      }
    }
    state.setRebalanceMode(rebalanceMode);
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

  public static IdealState getTableIdealState(HelixManager manager, String resourceName) {
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
        brokerIdealState.setPartitionState(resourceTag, instance, DROPPED);
      }
      helixAdmin.setResourceIdealState(helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, brokerIdealState);
    }
    LOGGER.info("Trying to remove resource from idealstats");
    if (brokerIdealState.getPartitionSet().contains(resourceTag)) {
      brokerIdealState.getPartitionSet().remove(resourceTag);
      helixAdmin.setResourceIdealState(helixClusterName, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, brokerIdealState);
    }
  }

  public static Set<String> getOnlineInstanceFromExternalView(ExternalView resourceExternalView) {
    Set<String> instanceSet = new HashSet<String>();
    if (resourceExternalView != null) {
      for (String partition : resourceExternalView.getPartitionSet()) {
        Map<String, String> stateMap = resourceExternalView.getStateMap(partition);
        for (String instance : stateMap.keySet()) {
          if (stateMap.get(instance).equalsIgnoreCase(ONLINE)) {
            instanceSet.add(instance);
          }
        }
      }
    }
    return instanceSet;
  }

  public static Set<String> getOfflineInstanceFromExternalView(ExternalView resourceExternalView) {
    Set<String> instanceSet = new HashSet<String>();
    for (String partition : resourceExternalView.getPartitionSet()) {
      Map<String, String> stateMap = resourceExternalView.getStateMap(partition);
      for (String instance : stateMap.keySet()) {
        if (stateMap.get(instance).equalsIgnoreCase(OFFLINE)) {
          instanceSet.add(instance);
        }
      }
    }
    return instanceSet;
  }
}
