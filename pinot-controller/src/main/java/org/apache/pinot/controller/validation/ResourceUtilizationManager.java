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
package org.apache.pinot.controller.validation;

import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.controller.ControllerConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ResourceUtilizationManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceUtilizationManager.class);

  private final boolean _isResourceUtilizationCheckEnabled;
  private final DiskUtilizationChecker _diskUtilizationChecker;

  public ResourceUtilizationManager(ControllerConf controllerConf, DiskUtilizationChecker diskUtilizationChecker) {
    _isResourceUtilizationCheckEnabled = controllerConf.isResourceUtilizationCheckEnabled();
    LOGGER.info("Resource utilization check is: {}", _isResourceUtilizationCheckEnabled ? "enabled" : "disabled");
    _diskUtilizationChecker = diskUtilizationChecker;
  }

  public boolean isResourceUtilizationWithinLimits(String tableNameWithType) {
    if (!_isResourceUtilizationCheckEnabled) {
      return true;
    }
    if (StringUtils.isEmpty(tableNameWithType)) {
      throw new IllegalArgumentException("Table name found to be null or empty while checking resource utilization.");
    }
    LOGGER.info("Checking resource utilization for table: {}", tableNameWithType);
    return _diskUtilizationChecker.isDiskUtilizationWithinLimits(tableNameWithType);
  }
}
