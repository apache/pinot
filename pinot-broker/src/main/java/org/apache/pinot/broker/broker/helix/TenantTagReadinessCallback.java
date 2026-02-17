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
package org.apache.pinot.broker.broker.helix;

import java.util.List;
import org.apache.helix.HelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.spi.utils.CommonConstants.Helix;

import static java.util.Objects.requireNonNull;


/**
 * Service status callback that checks whether the broker has valid tenant tags assigned.
 * Returns STARTING if the broker only has the untagged broker instance tag or no valid broker tags.
 * Returns GOOD once the broker has at least one valid tenant-specific broker tag (ending with _BROKER).
 */
public class TenantTagReadinessCallback implements ServiceStatus.ServiceStatusCallback {

  private static final String STATUS_INSTANCE_CONFIG_NOT_FOUND = "Instance config not found";
  private static final String STATUS_NO_TAGS_ASSIGNED = "No tags assigned to broker instance";
  private static final String STATUS_NO_VALID_BROKER_TAGS = "No valid tenant broker tags";

  private final HelixManager _helixManager;
  private final String _clusterName;
  private final String _instanceId;
  private volatile ServiceStatus.Status _serviceStatus = ServiceStatus.Status.STARTING;

  public TenantTagReadinessCallback(HelixManager helixManager, String clusterName, String instanceId) {
    _helixManager = requireNonNull(helixManager, "helixManager");
    _clusterName = requireNonNull(clusterName, "clusterName");
    _instanceId = requireNonNull(instanceId, "instanceId");
  }

  @Override
  public synchronized ServiceStatus.Status getServiceStatus() {
    // Return cached GOOD status to avoid re-checking
    if (_serviceStatus == ServiceStatus.Status.GOOD) {
      return _serviceStatus;
    }

    InstanceConfig instanceConfig =
        _helixManager.getConfigAccessor().getInstanceConfig(_clusterName, _instanceId);

    if (instanceConfig == null) {
      return ServiceStatus.Status.STARTING;
    }

    List<String> tags = instanceConfig.getTags();

    if (tags == null || tags.isEmpty()) {
      return ServiceStatus.Status.STARTING;
    }

    // Check if any tag is a valid broker tenant tag (ends with _BROKER)
    // and is not just the untagged broker instance
    for (String tag : tags) {
      if (TagNameUtils.isBrokerTag(tag) && !Helix.UNTAGGED_BROKER_INSTANCE.equals(tag)) {
        _serviceStatus = ServiceStatus.Status.GOOD;
        return _serviceStatus;
      }
    }

    return ServiceStatus.Status.STARTING;
  }

  @Override
  public synchronized String getStatusDescription() {
    if (_serviceStatus == ServiceStatus.Status.GOOD) {
      return ServiceStatus.STATUS_DESCRIPTION_NONE;
    }

    InstanceConfig instanceConfig =
        _helixManager.getConfigAccessor().getInstanceConfig(_clusterName, _instanceId);

    if (instanceConfig == null) {
      return STATUS_INSTANCE_CONFIG_NOT_FOUND;
    }

    List<String> tags = instanceConfig.getTags();

    if (tags == null || tags.isEmpty()) {
      return STATUS_NO_TAGS_ASSIGNED;
    }

    // Check if any tag is a valid broker tenant tag
    for (String tag : tags) {
      if (TagNameUtils.isBrokerTag(tag) && !Helix.UNTAGGED_BROKER_INSTANCE.equals(tag)) {
        return ServiceStatus.STATUS_DESCRIPTION_NONE;
      }
    }

    return STATUS_NO_VALID_BROKER_TAGS + ". Current tags: " + tags;
  }
}
