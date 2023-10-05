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
package org.apache.pinot.controller.helix.core.rebalance;

/**
 * Track the job configs and retry numbers as part of the job ZK metadata for future retry.
 */
public class TableRebalanceRetryConfig {
  private String _originalJobId;
  private int _retryNum;
  private RebalanceConfig _config;

  public static TableRebalanceRetryConfig forInitialRun(String originalJobId, RebalanceConfig config) {
    TableRebalanceRetryConfig rc = new TableRebalanceRetryConfig();
    rc.setOriginalJobId(originalJobId);
    rc.setConfig(config);
    rc.setRetryNum(0);
    return rc;
  }

  public static TableRebalanceRetryConfig forRetryRun(String originalJobId, RebalanceConfig config, int retryNum) {
    TableRebalanceRetryConfig rc = new TableRebalanceRetryConfig();
    rc.setOriginalJobId(originalJobId);
    rc.setConfig(config);
    rc.setRetryNum(retryNum);
    return rc;
  }

  public int getRetryNum() {
    return _retryNum;
  }

  public void setRetryNum(int retryNum) {
    _retryNum = retryNum;
  }

  public String getOriginalJobId() {
    return _originalJobId;
  }

  public void setOriginalJobId(String originalJobId) {
    _originalJobId = originalJobId;
  }

  public RebalanceConfig getConfig() {
    return _config;
  }

  public void setConfig(RebalanceConfig config) {
    _config = config;
  }

  @Override
  public String toString() {
    return "TableRebalanceRetryConfig{" + "_originalJobId='" + _originalJobId + '\'' + ", _retryNum=" + _retryNum
        + ", _config=" + _config + '}';
  }
}
