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

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.controller.ControllerConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ResourceUtilizationManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceUtilizationManager.class);

  private final boolean _isResourceUtilizationCheckEnabled;
  private final List<UtilizationChecker> _utilizationCheckers;

  public ResourceUtilizationManager(ControllerConf controllerConf, List<UtilizationChecker> utilizationCheckers) {
    _isResourceUtilizationCheckEnabled = controllerConf.isResourceUtilizationCheckEnabled();
    LOGGER.info("Resource utilization check is: {}, with {} resource utilization checkers",
        _isResourceUtilizationCheckEnabled ? "enabled" : "disabled", utilizationCheckers.size());
    _utilizationCheckers = utilizationCheckers;
  }

  /**
   * Returns the status of the resource utilization check across all UtilizationCheckers
   * @param tableNameWithType table name with type
   * @param purpose the purpose of the utilization check
   * @return CheckResult, FALSE if even one resource utilization checker has returned FALSE, STALE if the result cannot
   *         be determined for even one UtilizationChecker and all the others are also STALE or TRUE, and TRUE if
   *         resource utilization is within limits for all UtilizationCheckers
   */
  public UtilizationChecker.CheckResult isResourceUtilizationWithinLimits(String tableNameWithType,
      UtilizationChecker.CheckPurpose purpose) {
    if (!_isResourceUtilizationCheckEnabled) {
      return UtilizationChecker.CheckResult.TRUE;
    }
    if (StringUtils.isEmpty(tableNameWithType)) {
      throw new IllegalArgumentException("Table name found to be null or empty while checking resource utilization.");
    }
    LOGGER.info("Checking resource utilization for table: {}", tableNameWithType);
    UtilizationChecker.CheckResult overallIsResourceUtilizationWithinLimits = UtilizationChecker.CheckResult.TRUE;
    for (UtilizationChecker utilizationChecker : _utilizationCheckers) {
      UtilizationChecker.CheckResult isResourceUtilizationWithinLimits =
          utilizationChecker.isResourceUtilizationWithinLimits(tableNameWithType, purpose);
      LOGGER.info("For utilization checker: {}, isResourceUtilizationWithinLimits: {}, purpose: {}",
          utilizationChecker.getName(), isResourceUtilizationWithinLimits, purpose);
      if (isResourceUtilizationWithinLimits.equals(UtilizationChecker.CheckResult.FALSE)) {
        // If any UtilizationChecker returns FALSE, we should mark the overall as FALSE. FALSE should always have
        // priority over other results
        overallIsResourceUtilizationWithinLimits = UtilizationChecker.CheckResult.FALSE;
      } else if (overallIsResourceUtilizationWithinLimits.equals(UtilizationChecker.CheckResult.TRUE)
          && isResourceUtilizationWithinLimits.equals(UtilizationChecker.CheckResult.STALE)) {
        // If we haven't already updated the overall to a value other than TRUE, and we get a STALE result,
        // update the overall to STALE. Should not update to STALE if we have set the overall to FALSE.
        overallIsResourceUtilizationWithinLimits = UtilizationChecker.CheckResult.STALE;
      }
    }
    return overallIsResourceUtilizationWithinLimits;
  }
}
