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
//package org.apache.pinot.controller.workload.splitter;
//
//import org.apache.pinot.spi.config.workload.CostSplit;
//import org.apache.pinot.spi.config.workload.EnforcementProfile;
//import org.apache.pinot.spi.config.workload.InstanceCost;
//import org.apache.pinot.spi.config.workload.NodeConfig;
//import org.apache.pinot.spi.config.workload.PropagationScheme;
//import org.checkerframework.checker.units.qual.C;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//public class TableCostSplitter {
//
//  public Map<String, InstanceCost> computeInstanceCostMap(NodeConfig nodeConfig, Set<String> instances) {
//    PropagationScheme propagationScheme = nodeConfig.getPropagationScheme();
//    long totalCpuCostNs = nodeConfig.getEnforcementProfile().getCpuCostNs();
//    long totalMemoryCostBytes = nodeConfig.getEnforcementProfile().getMemoryCostBytes();
//
//    Map<String, InstanceCost> instanceCostMap = new HashMap<>();
//    for (String)
//
//
//    return new HashMap<>();
//  }
//
//  public InstanceCost computeInstanceCost(CostSplit costSplit, int numInstances) {
//    long cpuCostNs = costSplit.getCpuCostNs() / numInstances;
//    long memoryCostBytes = costSplit.getMemoryCostBytes() / numInstances;
//
//    return new InstanceCost(cpuCostNs, memoryCostBytes);
//  }
//}
