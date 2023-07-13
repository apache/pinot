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
package org.apache.pinot.controller.api.resources;

import org.apache.pinot.controller.helix.core.rebalance.TenantRebalanceProgressStats;


public class TenantRebalanceJobStatusResponse {
  private long _timeElapsedSinceStartInSeconds;
  private TenantRebalanceProgressStats _tenantRebalanceProgressStats;

  public long getTimeElapsedSinceStartInSeconds() {
    return _timeElapsedSinceStartInSeconds;
  }

  public void setTimeElapsedSinceStartInSeconds(long timeElapsedSinceStartInSeconds) {
    _timeElapsedSinceStartInSeconds = timeElapsedSinceStartInSeconds;
  }

  public TenantRebalanceProgressStats getTenantRebalanceProgressStats() {
    return _tenantRebalanceProgressStats;
  }

  public void setTenantRebalanceProgressStats(TenantRebalanceProgressStats tenantRebalanceProgressStats) {
    _tenantRebalanceProgressStats = tenantRebalanceProgressStats;
  }
}
