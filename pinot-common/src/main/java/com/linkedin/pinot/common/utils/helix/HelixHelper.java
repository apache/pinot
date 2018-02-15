/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.common.utils.retry.RetryPolicy;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;
import org.I0Itec.zkclient.exception.ZkBadVersionException;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HelixHelper {
  private static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicies.exponentialBackoffRetryPolicy(5, 1000L, 2.0f);
  private static final Logger LOGGER = LoggerFactory.getLogger(HelixHelper.class);
  private static final int MAX_PARTITION_COUNT_IN_UNCOMPRESSED_IDEAL_STATE = 1000;

  private static final String ONLINE = "ONLINE";
  private static final String OFFLINE = "OFFLINE";

  public static final String BROKER_RESOURCE = CommonConstants.Helix.BROKER_RESOURCE_INSTANCE;

  /**
   * Updates the ideal state, retrying if necessary in case of concurrent updates to the ideal state.
   *
   * @param helixManager The HelixManager used to interact with the Helix cluster
   * @param resourceName The resource for which to update the ideal state
   * @param updater A function that returns an updated ideal state given an input ideal state
   */
  // TODO: since updater always update ideal state in place, it should return boolean indicating whether the ideal state get changed.
  public static void updateIdealState(final HelixManager helixManager, final String resourceName,
      final Function<IdealState, IdealState> updater, RetryPolicy policy) {
    try {
      policy.attempt(new Callable<Boolean>() {
        @Override
        public Boolean call() {
          HelixDataAccessor dataAccessor = helixManager.getHelixDataAccessor();
          PropertyKey idealStateKey = dataAccessor.keyBuilder().idealStates(resourceName);
          IdealState idealState = dataAccessor.getProperty(idealStateKey);

          // Make a copy of the the idealState above to pass it to the updater
          // NOTE: new IdealState(idealState.getRecord()) does not work because it's shallow copy for map fields and
          // list fields
          ZNRecordSerializer znRecordSerializer = new ZNRecordSerializer();
          IdealState idealStateCopy = new IdealState(
              (ZNRecord) znRecordSerializer.deserialize(znRecordSerializer.serialize(idealState.getRecord())));

          IdealState updatedIdealState;
          try {
            updatedIdealState = updater.apply(idealStateCopy);
          } catch (Exception e) {
            LOGGER.error("Caught exception while updating ideal state for resource: {}", resourceName, e);
            return false;
          }

          // If there are changes to apply, apply them
          if (!EqualityUtils.isEqual(idealState, updatedIdealState) && updatedIdealState != null) {

            // If the ideal state is large enough, enable compression
            if (MAX_PARTITION_COUNT_IN_UNCOMPRESSED_IDEAL_STATE < updatedIdealState.getPartitionSet().size()) {
              updatedIdealState.getRecord().setBooleanField("enableCompression", true);
            }

            // Check version and set ideal state
            try {
              if (dataAccessor.getBaseDataAccessor()
                  .set(idealStateKey.getPath(), updatedIdealState.getRecord(), idealState.getRecord().getVersion(),
                      AccessOption.PERSISTENT)) {
                return true;
              } else {
                LOGGER.warn("Failed to update ideal state for resource: {}", resourceName);
                return false;
              }
            } catch (ZkBadVersionException e) {
              LOGGER.warn("Version changed while updating ideal state for resource: {}", resourceName);
              return false;
            } catch (Exception e) {
              boolean idealStateIsCompressed =
                  updatedIdealState.getRecord().getBooleanField("enableCompression", false);
              LOGGER.warn("Caught exception while updating ideal state for resource: {} (compressed={})", resourceName,
                  idealStateIsCompressed, e);
              return false;
            }
          } else {
            LOGGER.warn("Idempotent or null ideal state update for resource {}, skipping update.", resourceName);
            return true;
          }
        }
      });
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while updating ideal state for resource: " + resourceName, e);
    }
  }

  /**
   * Returns all instances for the given cluster.
   *
   * @param helixAdmin The HelixAdmin object used to interact with the Helix cluster
   * @param clusterName Name of the cluster for which to get all the instances for.
   * @return Returns a List of strings containing the instance names for the given cluster.
   */
  public static List<String> getAllInstances(HelixAdmin helixAdmin, String clusterName) {
    return helixAdmin.getInstancesInCluster(clusterName);
  }

  /**
   * Returns all instances for the given resource.
   *
   * @param idealState IdealState of the resource for which to return the instances of.
   * @return Returns a Set of strings containing the instance names for the given cluster.
   */
  public static Set<String> getAllInstancesForResource(IdealState idealState) {
    final Set<String> instances = new HashSet<String>();

    for (final String partition : idealState.getPartitionSet()) {
      for (final String instance : idealState.getInstanceSet(partition)) {
        instances.add(instance);
      }
    }
    return instances;
  }

  /**
   * Toggle the state of the instance between OFFLINE and ONLINE.
   *
   * @param instanceName Name of the instance for which to toggle the state.
   * @param clusterName Name of the cluster to which the instance belongs.
   * @param admin HelixAdmin to access the cluster.
   * @param enable Set enable to true for ONLINE and FALSE for OFFLINE.
   */
  public static void setInstanceState(String instanceName, String clusterName, HelixAdmin admin, boolean enable) {
    admin.enableInstance(clusterName, instanceName, enable);
  }

  public static void setStateForInstanceList(List<String> instances, String clusterName, HelixAdmin admin,
      boolean enable) {
    for (final String instance : instances) {
      setInstanceState(instance, clusterName, admin, enable);
    }
  }

  public static void setStateForInstanceSet(Set<String> instances, String clusterName, HelixAdmin admin, boolean enable) {
    for (final String instanceName : instances) {
      setInstanceState(instanceName, clusterName, admin, enable);
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

  public static ExternalView getExternalViewForResource(HelixAdmin admin, String clusterName, String resourceName) {
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

  /**
   * Remove a resource (offline/realtime table) from the Broker's ideal state.
   *
   * @param helixManager The HelixManager object for accessing helix cluster.
   * @param resourceTag Name of the resource that needs to be removed from Broker ideal state.
   */
  public static void removeResourceFromBrokerIdealState(HelixManager helixManager, final String resourceTag) {
    Function<IdealState, IdealState> updater = new Function<IdealState, IdealState>() {
      @Override
      public IdealState apply(IdealState idealState) {
        if (idealState.getPartitionSet().contains(resourceTag)) {
          idealState.getPartitionSet().remove(resourceTag);
          return idealState;
        } else {
          return null;
        }
      }
    };

    // Removing partitions from ideal state
    LOGGER.info("Trying to remove resource {} from idealstate", resourceTag);
    HelixHelper.updateIdealState(helixManager, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE, updater,
        DEFAULT_RETRY_POLICY);
  }

  /**
   * Returns the set of online instances from external view.
   *
   * @param resourceExternalView External view for the resource.
   * @return Set&lt;String&gt; of online instances in the external view for the resource.
   */
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

  /**
   * Get a set of offline instance from the external view of the resource.
   *
   * @param resourceExternalView External view of the resource
   * @return Set of string instance names of the offline instances in the external view.
   */
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

  /**
   * Remove the segment from the cluster.
   *
   * @param helixManager The HelixManager object to access the helix cluster.
   * @param tableName Name of the table to which the new segment is to be added.
   * @param segmentName Name of the new segment to be added
   */
  public static void removeSegmentFromIdealState(HelixManager helixManager, String tableName, final String segmentName) {
    Function<IdealState, IdealState> updater = new Function<IdealState, IdealState>() {
      @Override
      public IdealState apply(IdealState idealState) {
        if (idealState == null) {
          return idealState;
        }
        // partitionSet is never null but let's be defensive anyway
        Set<String> partitionSet = idealState.getPartitionSet();
        if (partitionSet != null) {
          partitionSet.remove(segmentName);
        }
        return idealState;
      }
    };

    updateIdealState(helixManager, tableName, updater, DEFAULT_RETRY_POLICY);
  }

  public static void removeSegmentsFromIdealState(HelixManager helixManager, String tableName, final List<String> segments) {
    Function<IdealState, IdealState> updater = new Function<IdealState, IdealState>() {
      @Nullable
      @Override
      public IdealState apply(@Nullable IdealState idealState) {
        if (idealState == null) {
          return idealState;
        }
        // partitionSet is never null but let's be defensive anyway
        Set<String> partitionSet = idealState.getPartitionSet();
        if (partitionSet != null) {
          partitionSet.removeAll(segments);
        }
        return idealState;
      }
    };
    updateIdealState(helixManager, tableName, updater, DEFAULT_RETRY_POLICY);
  }

  /**
   * Add the new specified segment to the idealState of the specified table in the specified cluster.
   *
   * @param helixManager The HelixManager object to access the helix cluster.
   * @param tableNameWithType Name of the table to which the new segment is to be added.
   * @param segmentName Name of the new segment to be added
   * @param assignedInstances List of assigned instances
   */
  public static void addSegmentToIdealState(HelixManager helixManager, final String tableNameWithType,
      final String segmentName, final List<String> assignedInstances) {

    Function<IdealState, IdealState> updater = new Function<IdealState, IdealState>() {
      @Override
      public IdealState apply(IdealState idealState) {
        Set<String> partitions = idealState.getPartitionSet();
        if (partitions.contains(segmentName)) {
          LOGGER.warn("Segment already exists in the ideal state for segment: {} of table: {}, do not update",
              segmentName, tableNameWithType);
        } else {
          if (assignedInstances.isEmpty()) {
            LOGGER.warn("No instance assigned for segment: {} of table: {}", segmentName, tableNameWithType);
          } else {
            int numPartitions = partitions.size() + 1;
            for (String instance : assignedInstances) {
              idealState.setPartitionState(segmentName, instance, ONLINE);
            }
            idealState.setNumPartitions(numPartitions);
          }
        }
        return idealState;
      }
    };

    updateIdealState(helixManager, tableNameWithType, updater, DEFAULT_RETRY_POLICY);
  }

  public static List<String> getEnabledInstancesWithTag(HelixAdmin helixAdmin, String helixClusterName,
      String instanceTag) {
    List<String> instances = helixAdmin.getInstancesInCluster(helixClusterName);
    List<String> enabledInstances = new ArrayList<>();
    for (String instance : instances) {
      try {
        InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(helixClusterName, instance);
        if (instanceConfig.containsTag(instanceTag) && instanceConfig.getInstanceEnabled()) {
          enabledInstances.add(instance);
        }
      } catch (Exception e) {
        LOGGER.error("Caught exception while fetching instance config for instance: {}", instance, e);
      }
    }
    return enabledInstances;
  }
}
