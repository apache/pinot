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
package org.apache.pinot.benchmark;

import org.apache.commons.configuration.PropertiesConfiguration;


public class PinotBenchConf extends PropertiesConfiguration {

  private static final String PINOT_BENCH_HOST = "pinot.bench.host";
  private static final String PINOT_BENCH_PORT = "pinot.bench.port";

  private static final String PERF_CONTROLLER_HOST = "perf.controller.host";
  private static final String PERF_CONTROLLER_PORT = "perf.controller.port";

  private static final String PROD_CONTROLLER_HOST = "prod.controller.host";
  private static final String PROD_CONTROLLER_PORT = "prod.controller.port";

  private static final String CLUSTER_NAME = "pinot.bench.cluster.name";
  private static final String ZK_STR = "pinot.bench.zk.str";

  private static final String TABLE_RETENTION_MANAGER_INITIAL_DELAY_IN_SECONDS =
      "pinot.bench.table.retention.initialDelayInSeconds";
  private static final String TABLE_RETENTION_MANAGER_FREQUENCY_IN_SECONDS =
      "pinot.bench.table.retention.frequencyInSeconds";

  private static final String LOCAL_TEMP_DIR = "controller.local.temp.dir";

  public void setPinotBenchHost(String pinotBenchHost) {
    setProperty(PINOT_BENCH_HOST, pinotBenchHost);
  }

  public String getPinotBenchHost() {
    return (String) getProperty(PINOT_BENCH_HOST);
  }

  public void setPinotBenchPort(int pinotBenchPort) {
    setProperty(PINOT_BENCH_PORT, pinotBenchPort);
  }

  public int getPinotBenchPort() {
    return getInt(PINOT_BENCH_PORT);
  }

  public void setPerfControllerHost(String perfControllerHost) {
    setProperty(PERF_CONTROLLER_HOST, perfControllerHost);
  }

  public String getPerfControllerHost() {
    return (String) getProperty(PERF_CONTROLLER_HOST);
  }

  public void setPerfControllerPort(int perfControllerPort) {
    setProperty(PERF_CONTROLLER_PORT, perfControllerPort);
  }

  public int getPerfControllerPort() {
    return getInt(PERF_CONTROLLER_PORT);
  }

  public void setProdControllerHost(String prodControllerHost) {
    setProperty(PROD_CONTROLLER_HOST, prodControllerHost);
  }

  public String getProdControllerHost() {
    return (String) getProperty(PROD_CONTROLLER_HOST);
  }

  public void setProdControllerPort(int prodControllerPort) {
    setProperty(PROD_CONTROLLER_PORT, prodControllerPort);
  }

  public int getProdControllerPort() {
    return getInt(PROD_CONTROLLER_PORT, -1);
  }

  public void setClusterName(String clusterName) {
    setProperty(CLUSTER_NAME, clusterName);
  }

  public String getClusterName() {
    return (String) getProperty(CLUSTER_NAME);
  }

  public void setZkStr(String zkStr) {
    setProperty(ZK_STR, zkStr);
  }

  public String getZkStr() {
    return (String) getProperty(ZK_STR);
  }

  public void setTableRetentionManagerInitialDelayInSeconds(long tableRetentionManagerInitialDelayInSeconds) {
    setProperty(TABLE_RETENTION_MANAGER_INITIAL_DELAY_IN_SECONDS, tableRetentionManagerInitialDelayInSeconds);
  }

  public long getTableRetentionManagerInitialDelayInSeconds() {
    return getLong(TABLE_RETENTION_MANAGER_INITIAL_DELAY_IN_SECONDS);
  }

  public void setTableRetentionManagerFrequencyInSeconds(long tableRetentionManagerFrequencyInSeconds) {
    setProperty(TABLE_RETENTION_MANAGER_FREQUENCY_IN_SECONDS, tableRetentionManagerFrequencyInSeconds);
  }

  public long getTableRetentionManagerFrequencyInSeconds() {
    return getLong(TABLE_RETENTION_MANAGER_FREQUENCY_IN_SECONDS);
  }
}
