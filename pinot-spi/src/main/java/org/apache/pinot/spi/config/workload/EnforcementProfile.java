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
package org.apache.pinot.spi.config.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EnforcementProfile extends BaseJsonConfig {

  private static final String BROKER_COST = "brokerCost";
  private static final String SERVER_COST = "serverCost";


  @JsonPropertyDescription("Workload cost for the broker")
  private WorkloadCost _brokerCost;

  @JsonPropertyDescription("Workload cost for the server")
  private WorkloadCost _serverCost;

  @JsonCreator
  public EnforcementProfile(@JsonProperty(BROKER_COST) @Nullable WorkloadCost brokerCost,
      @JsonProperty(SERVER_COST) @Nullable WorkloadCost serveCost) {
    _brokerCost = brokerCost;
    _serverCost = serveCost;
  }

  public WorkloadCost getBrokerCost() {
    return _brokerCost;
  }

  public WorkloadCost getServerCost() {
    return _serverCost;
  }

  public void setBrokerCost(WorkloadCost brokerCost) {
    _brokerCost = brokerCost;
  }

  public void setServerCost(WorkloadCost serverCost) {
    _serverCost = serverCost;
  }
}
