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

public class InstanceCost {

  private double _cpuCost;
  private double _memoryCost;
  private long _enforcementPeriodMillis;

  public InstanceCost(double cpuCost, double memoryCost, long enforcementPeriodMillis) {
    _cpuCost = cpuCost;
    _memoryCost = memoryCost;
    _enforcementPeriodMillis = enforcementPeriodMillis;
  }

  public double getCpuCost() {
    return _cpuCost;
  }

  public double getMemoryCost() {
    return _memoryCost;
  }

  public long getEnforcementPeriodMillis() {
    return _enforcementPeriodMillis;
  }

  public void setCpuCost(double cpuCost) {
    _cpuCost = cpuCost;
  }

  public void setMemoryCost(double memoryCost) {
    _memoryCost = memoryCost;
  }

  public void setEnforcementPeriodMillis(long enforcementPeriodMillis) {
    _enforcementPeriodMillis = enforcementPeriodMillis;
  }
}
