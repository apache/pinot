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

package com.linkedin.pinot.common.utils;

import com.linkedin.pinot.common.config.TableNameBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to obtain the status of the Pinot instance running in this JVM.
 */
public class ServiceStatus {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceStatus.class);

  public enum Status {
    STARTING, GOOD, BAD
  }

  public static String STATUS_DESCRIPTION_NONE = "None";
  public static String STATUS_DESCRIPTION_INIT = "Init";

  /**
   * Callback that returns the status of the service.
   */
  public interface ServiceStatusCallback {
    Status getServiceStatus();
    String getStatusDescription();
  }

  private static ServiceStatusCallback serviceStatusCallback = null;

  public static void setServiceStatusCallback(ServiceStatusCallback serviceStatusCallback) {
    ServiceStatus.serviceStatusCallback = serviceStatusCallback;
  }

  public static Status getServiceStatus() {
    if (serviceStatusCallback == null) {
      return Status.STARTING;
    } else {
      try {
        return serviceStatusCallback.getServiceStatus();
      } catch (Exception e) {
        LOGGER.warn("Caught exception while reading the service status", e);
        return Status.BAD;
      }
    }
  }

  public static String getStatusDescription() {
    if (serviceStatusCallback == null) {
      return STATUS_DESCRIPTION_INIT;
    } else {
      try {
        return serviceStatusCallback.getStatusDescription();
      } catch (Exception e) {
        return "Exception: " + e.getMessage();
      }
    }
  }

  public static class MultipleCallbackServiceStatusCallback implements ServiceStatusCallback {
    private final List<? extends ServiceStatusCallback> _statusCallbacks;

    public MultipleCallbackServiceStatusCallback(List<? extends ServiceStatusCallback> statusCallbacks) {
      _statusCallbacks = statusCallbacks;
    }

    @Override
    public Status getServiceStatus() {
      // Iterate through all callbacks, returning the first non GOOD one as the service status
      for (ServiceStatusCallback statusCallback : _statusCallbacks) {
        final Status serviceStatus = statusCallback.getServiceStatus();
        if (serviceStatus != Status.GOOD) {
          return serviceStatus;
        }
      }

      // All callbacks report good, therefore we're good too
      return Status.GOOD;
    }

    public String getStatusDescription() {
      StringBuilder statusDescription = new StringBuilder();
      for (ServiceStatusCallback statusCallback : _statusCallbacks) {
        statusDescription.append(
            statusCallback.getClass().getSimpleName() + ":" + statusCallback.getStatusDescription() + ";"
        );
      }
      return statusDescription.toString();
    }
  }

  /**
   * Service status callback that compares ideal state with another Helix state. Used to share most of the logic between
   * the ideal state/external view comparison and ideal state/current state comparison.
   */
  private static abstract class IdealStateMatchServiceStatusCallback<T> implements ServiceStatusCallback {
    protected String _clusterName;
    protected String _instanceName;
    protected List<String> _resourcesToMonitor;
    protected boolean _finishedStartingUp = false;
    protected HelixManager _helixManager;
    protected HelixAdmin _helixAdmin;
    protected HelixDataAccessor _helixDataAccessor;
    protected Builder _keyBuilder;
    protected Set<String> _resourcesDoneStartingUp = new HashSet<>();
    private String _statusDescription = STATUS_DESCRIPTION_INIT;

    public IdealStateMatchServiceStatusCallback(HelixManager helixManager, String clusterName, String instanceName) {
      _helixManager = helixManager;
      _clusterName = clusterName;
      _instanceName = instanceName;

      if (_helixManager != null) {
        _helixAdmin = _helixManager.getClusterManagmentTool();
        _helixDataAccessor = _helixManager.getHelixDataAccessor();
        _keyBuilder = _helixDataAccessor.keyBuilder();
      }

      // Make a list of the resources to monitor
      _resourcesToMonitor = new ArrayList<>();

      for (String resource : _helixAdmin.getResourcesInCluster(_clusterName)) {
        // Only monitor table resources and broker resource
        if (!TableNameBuilder.isTableResource(resource) && !resource.equals(
            CommonConstants.Helix.BROKER_RESOURCE_INSTANCE)) {
          continue;
        }
        final IdealState idealState = getResourceIdealState(resource);
        for (String partitionInResource : idealState.getPartitionSet()) {
          if (idealState.getInstanceSet(partitionInResource).contains(_instanceName)) {
            _resourcesToMonitor.add(resource);
            break;
          }
        }
      }

      LOGGER.info("Monitoring resources {} for start up of instance {}", _resourcesToMonitor, _instanceName);
    }

    public IdealStateMatchServiceStatusCallback(HelixManager helixManager, String clusterName, String instanceName,
        List<String> resourcesToMonitor) {
      _helixManager = helixManager;
      _clusterName = clusterName;
      _instanceName = instanceName;

      if (_helixManager != null) {
        _helixAdmin = _helixManager.getClusterManagmentTool();
        _helixDataAccessor = _helixManager.getHelixDataAccessor();
        _keyBuilder = _helixDataAccessor.keyBuilder();
      }

      _resourcesToMonitor = resourcesToMonitor;

      LOGGER.info("Monitoring resources {} for start up of instance {}", _resourcesToMonitor, _instanceName);
    }

    public String getStatusDescription() {
      return _statusDescription;
    }

    protected abstract T getState(String resourceName);

    protected abstract Map<String, String> getPartitionStateMap(T state);

    @Override
    public Status getServiceStatus() {
      if (_finishedStartingUp) {
        return Status.GOOD;
      }
      int index = 0;
      final int totalResourceCount = _resourcesToMonitor.size();

      for (String resourceToMonitor : _resourcesToMonitor) {
        final String completedCountStr = "(" + resourceToMonitor + ":" + index + "/" + totalResourceCount + ")";
        // If the instance is already done starting up, skip checking its state
        if (_resourcesDoneStartingUp.contains(resourceToMonitor)) {
          continue;
        }

        IdealState idealState = getResourceIdealState(resourceToMonitor);
        T helixState = getState(resourceToMonitor);

        if (idealState == null || helixState == null) {
          _statusDescription = "idealState or helixState is null" + completedCountStr;
          return Status.STARTING;
        }

        // If the resource is disabled, ignore it
        if (!idealState.isEnabled()) {
          continue;
        }

        Map<String, String> statePartitionStateMap = getPartitionStateMap(helixState);
        if(statePartitionStateMap.isEmpty() && !idealState.getPartitionSet().isEmpty()) {
          _statusDescription = "statePartitionStateMapSize=" + statePartitionStateMap.size() + ", idealStateSize="
              + idealState.getPartitionSet().size() + completedCountStr;
          return Status.STARTING;
        }

        // Check that all partitions that are supposed to be in any state other than OFFLINE have the same status in the
        // external view or went to ERROR state (which means that we tried to load the segments/resources but failed for
        // some reason)
        for (String partition : idealState.getPartitionSet()) {
          final String idealStateStatus = idealState.getInstanceStateMap(partition).get(_instanceName);

          // Skip this partition if it is not assigned to this instance or if the instance should be offline
          if (idealStateStatus == null || "OFFLINE".equals(idealStateStatus)) {
            continue;
          }

          // If this instance state is not in the current state, then it hasn't finished starting up
          if (!statePartitionStateMap.containsKey(partition)) {
            _statusDescription = "statePartitionStateMap does not have " + partition + completedCountStr;
            return Status.STARTING;
          }

          String currentStateStatus = statePartitionStateMap.get(partition);

          // If the instance state is not ERROR and is not the same as what's expected from the ideal state, then it
          // hasn't finished starting up
          if (!"ERROR".equals(currentStateStatus) && !idealStateStatus.equals(currentStateStatus)) {
            _statusDescription = partition +" currentStateStatus=" + currentStateStatus + ", idealStateStatus="
                + idealStateStatus + completedCountStr;
            return Status.STARTING;
          }
        }

        // Resource is done starting up, add it to the list of resources that are done starting up
        _resourcesDoneStartingUp.add(resourceToMonitor);
      }

      LOGGER.info("Instance {} has finished starting up", _instanceName);
      _finishedStartingUp = true;
      _statusDescription = STATUS_DESCRIPTION_NONE;
      return Status.GOOD;
    }

    protected IdealState getResourceIdealState(String resourceName) {
      return _helixAdmin.getResourceIdealState(_clusterName, resourceName);
    }
  }

  /**
   * Service status callback that reports starting until all resources relevant to this instance have a matching
   * external view and current state. This callback considers the ERROR state in the current view to be equivalent to
   * the ideal state value.
   */
  public static class IdealStateAndCurrentStateMatchServiceStatusCallback extends IdealStateMatchServiceStatusCallback<CurrentState> {
    public IdealStateAndCurrentStateMatchServiceStatusCallback(HelixManager helixManager, String clusterName, String instanceName) {
      super(helixManager, clusterName, instanceName);
    }

    public IdealStateAndCurrentStateMatchServiceStatusCallback(HelixManager helixManager, String clusterName, String instanceName,
        List<String> resourcesToMonitor) {
      super(helixManager, clusterName, instanceName, resourcesToMonitor);
    }

    @Override
    protected CurrentState getState(String resourceName) {
      LiveInstance liveInstance = _helixDataAccessor.getProperty(_keyBuilder.liveInstance(_instanceName));

      String sessionId = liveInstance.getSessionId();
      PropertyKey currentStateKey = _keyBuilder.currentState(_instanceName, sessionId, resourceName);
      CurrentState currentState = _helixDataAccessor.getProperty(currentStateKey);

      return currentState;
    }

    @Override
    protected Map<String, String> getPartitionStateMap(CurrentState state) {
      return state.getPartitionStateMap();
    }
  }

  /**
   * Service status callback that reports starting until all resources relevant to this instance have a matching
   * external view and ideal state. This callback considers the ERROR state in the external view to be equivalent to the
   * ideal state value.
   */
  public static class IdealStateAndExternalViewMatchServiceStatusCallback extends IdealStateMatchServiceStatusCallback<ExternalView> {
    public IdealStateAndExternalViewMatchServiceStatusCallback(HelixManager helixManager, String clusterName, String instanceName) {
      super(helixManager, clusterName, instanceName);
    }

    public IdealStateAndExternalViewMatchServiceStatusCallback(HelixManager helixManager, String clusterName, String instanceName,
        List<String> resourcesToMonitor) {
      super(helixManager, clusterName, instanceName, resourcesToMonitor);
    }

    @Override
    protected ExternalView getState(String resourceName) {
      return _helixAdmin.getResourceExternalView(_clusterName, resourceName);
    }

    @Override
    protected Map<String, String> getPartitionStateMap(ExternalView state) {
      Map<String, String> partitionState = new HashMap<>();

      for (String partition : state.getPartitionSet()) {
        Map<String, String> instanceStateMap = state.getStateMap(partition);
        if (instanceStateMap.containsKey(_instanceName)) {
          partitionState.put(partition, instanceStateMap.get(_instanceName));
        }
      }

      return partitionState;
    }
  }
}
