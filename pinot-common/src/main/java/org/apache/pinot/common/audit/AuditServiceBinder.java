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
package org.apache.pinot.common.audit;

import org.apache.pinot.common.config.DefaultClusterConfigChangeHandler;
import org.apache.pinot.spi.services.ServiceRole;
import org.glassfish.hk2.utilities.binding.AbstractBinder;


/**
 * HK2 Binder for audit-related services.
 * Handles initialization and registration of audit components.
 */
public class AuditServiceBinder extends AbstractBinder {
  private final DefaultClusterConfigChangeHandler _clusterConfigChangeHandler;
  private final ServiceRole _serviceRole;

  public AuditServiceBinder(DefaultClusterConfigChangeHandler clusterConfigChangeHandler, ServiceRole serviceRole) {
    _clusterConfigChangeHandler = clusterConfigChangeHandler;
    _serviceRole = serviceRole;
  }

  @Override
  protected void configure() {
    // Create and initialize AuditConfigManager
    AuditConfigManager auditConfigManager = new AuditConfigManager(_serviceRole);

    // Register with cluster config change handler
    _clusterConfigChangeHandler.registerClusterConfigChangeListener(auditConfigManager);
    bind(auditConfigManager).to(AuditConfigManager.class);

    bindAsContract(AuditIdentityResolver.class);
    bindAsContract(AuditRequestProcessor.class);
    bindAsContract(AuditUrlPathFilter.class);
  }
}
