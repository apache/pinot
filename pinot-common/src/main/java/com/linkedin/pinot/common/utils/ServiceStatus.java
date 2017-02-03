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

import java.util.ArrayList;
import java.util.List;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class to obtain the status of the Pinot instance running in this JVM.
 */
public class ServiceStatus {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceStatus.class);

  public enum Status {
    STARTING,
    GOOD,
    BAD
  }

  /**
   * Callback that returns the status of the service.
   */
  public interface ServiceStatusCallback {
    Status getServiceStatus();
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

  /**
   * Service status callback that reports starting until all resources relevant to this instance have a matching
   * external view and ideal state. This callback considers the ERROR state in the external view to be equivalent to the
   * ideal state value.
   */
  public static class IdealStateAndExternalViewMatchServiceStatusCallback implements ServiceStatusCallback {
    private HelixAdmin _helixAdmin;
    private String _clusterName;
    private String _instanceName;
    private List<String> _resourcesToMonitor;
    private boolean _finishedStartingUp = false;

    public IdealStateAndExternalViewMatchServiceStatusCallback(HelixAdmin helixAdmin, String clusterName,
        String instanceName) {
      _helixAdmin = helixAdmin;
      _clusterName = clusterName;
      _instanceName = instanceName;

      // Make a list of the resources to monitor
      _resourcesToMonitor = new ArrayList<>();

      for (String resource : _helixAdmin.getResourcesInCluster(_clusterName)) {
        final IdealState idealState = _helixAdmin.getResourceIdealState(_clusterName, resource);
        for (String partitionInResource : idealState.getPartitionSet()) {
          if (idealState.getInstanceSet(partitionInResource).contains(_instanceName)) {
            _resourcesToMonitor.add(resource);
            break;
          }
        }
      }

      LOGGER.info("Monitoring resources {} for start up of instance {}", _resourcesToMonitor, _instanceName);
    }

    public IdealStateAndExternalViewMatchServiceStatusCallback(HelixAdmin helixAdmin, String clusterName,
        String instanceName, List<String> resourcesToMonitor) {
      _helixAdmin = helixAdmin;
      _clusterName = clusterName;
      _instanceName = instanceName;
      _resourcesToMonitor = resourcesToMonitor;

      LOGGER.info("Monitoring resources {} for start up of instance {}", _resourcesToMonitor, _instanceName);
    }

    @Override
    public Status getServiceStatus() {
      if(_finishedStartingUp) {
        return Status.GOOD;
      }

      for (String resourceToMonitor : _resourcesToMonitor) {
        IdealState idealState = _helixAdmin.getResourceIdealState(_clusterName, resourceToMonitor);
        ExternalView externalView = _helixAdmin.getResourceExternalView(_clusterName, resourceToMonitor);

        if (idealState == null || externalView == null) {
          return Status.STARTING;
        }

        for (String partition : idealState.getPartitionSet()) {
          // If this partition is not in the external view, then it hasn't finished starting up
          if (!externalView.getPartitionSet().contains(partition)) {
            return Status.STARTING;
          }

          String idealStateStatus = idealState.getInstanceStateMap(partition).get(_instanceName);
          String externalViewStatus = externalView.getStateMap(partition).get(_instanceName);

          // Skip this partition if it is not assigned to this instance
          if (idealStateStatus == null) {
            continue;
          }

          // If this instance state is not in the external view, then it hasn't finished starting up
          if (externalViewStatus == null) {
            return Status.STARTING;
          }

          // If the instance state is not ERROR and is not the same as what's expected from the ideal state, then it
          // hasn't finished starting up
          if (!externalViewStatus.equals("ERROR") && !externalViewStatus.equals(idealStateStatus)) {
            return Status.STARTING;
          }
        }
      }

      LOGGER.info("Instance {} has finished starting up", _instanceName);
      _finishedStartingUp = true;
      return Status.GOOD;
    }
  }
}
