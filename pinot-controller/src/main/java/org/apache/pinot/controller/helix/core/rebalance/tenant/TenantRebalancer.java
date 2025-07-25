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
package org.apache.pinot.controller.helix.core.rebalance.tenant;

public interface TenantRebalancer {
  TenantRebalanceResult rebalance(TenantRebalanceConfig config);

  class TenantTableRebalanceJobContext {
    private final String _tableName;
    private final String _jobId;
    // Whether the rebalance should be done with downtime or minAvailableReplicas=0.
    private final boolean _withDowntime;

    /**
     * Create a context to run a table rebalance job with in a tenant rebalance operation.
     *
     * @param tableName The name of the table to rebalance.
     * @param jobId The job ID for the rebalance operation.
     * @param withDowntime Whether the rebalance should be done with downtime or minAvailableReplicas=0.
     * @return The result of the rebalance operation.
     */
    public TenantTableRebalanceJobContext(String tableName, String jobId, boolean withDowntime) {
      _tableName = tableName;
      _jobId = jobId;
      _withDowntime = withDowntime;
    }

    public String getJobId() {
      return _jobId;
    }

    public String getTableName() {
      return _tableName;
    }

    public boolean shouldRebalanceWithDowntime() {
      return _withDowntime;
    }
  }
}
