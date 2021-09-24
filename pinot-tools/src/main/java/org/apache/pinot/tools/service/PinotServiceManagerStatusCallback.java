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
package org.apache.pinot.tools.service;

import org.apache.pinot.common.utils.ServiceStatus;


public class PinotServiceManagerStatusCallback implements ServiceStatus.ServiceStatusCallback {
  private final PinotServiceManager _pinotServiceManager;

  public PinotServiceManagerStatusCallback(PinotServiceManager pinotServiceManager) {
    _pinotServiceManager = pinotServiceManager;
  }

  @Override
  public ServiceStatus.Status getServiceStatus() {
    if (_pinotServiceManager.isStarted()) {
      return ServiceStatus.Status.GOOD;
    } else {
      return ServiceStatus.Status.STARTING;
    }
  }

  @Override
  public String getStatusDescription() {
    if (_pinotServiceManager.isStarted()) {
      return ServiceStatus.STATUS_DESCRIPTION_STARTED;
    } else {
      return ServiceStatus.STATUS_DESCRIPTION_INIT;
    }
  }
}
