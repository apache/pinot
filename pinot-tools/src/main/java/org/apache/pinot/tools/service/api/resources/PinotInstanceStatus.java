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
package org.apache.pinot.tools.service.api.resources;

import java.util.Map;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.services.ServiceRole;


public class PinotInstanceStatus {
  private final ServiceRole _serviceRole;
  private final String _instanceId;
  private final Map<String, Object> _config;
  private final ServiceStatus.Status _serviceStatus;
  private final String _statusDescription;

  public PinotInstanceStatus(ServiceRole serviceRole, String instanceId, PinotConfiguration config,
      ServiceStatus.Status serviceStatus, String statusDescription) {
    _serviceRole = serviceRole;
    _instanceId = instanceId;
    _config = config.toMap();
    _serviceStatus = serviceStatus;
    _statusDescription = statusDescription;
  }

  public String getStatusDescription() {
    return _statusDescription;
  }

  public ServiceRole getServiceRole() {
    return _serviceRole;
  }

  public String getInstanceId() {
    return _instanceId;
  }

  public Map<String, Object> getConfig() {
    return _config;
  }

  public ServiceStatus.Status getServiceStatus() {
    return _serviceStatus;
  }
}
