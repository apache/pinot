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
package org.apache.pinot.compat.tests;

// TODO Support https, perhaps based on configuration
public class ClusterDescriptor {

  private static final String DEFAULT_HOST = "localhost";
  private static final String ZOOKEEPER_PORT = "2181";
  private static final String KAFKA_PORT = "19092";
  private static final String ZOOKEEPER_URL = String.format("http://%s:%s", DEFAULT_HOST, ZOOKEEPER_PORT);
  private static final String KAFKA_URL = String.format("http://%s:%s", DEFAULT_HOST, KAFKA_PORT);

  private String controllerPort = "9000";
  private String brokerQueryPort = "8099";
  private String serverAdminPort = "8097";

  private static final ClusterDescriptor INSTANCE = new ClusterDescriptor();

  public static ClusterDescriptor getInstance() {
    return INSTANCE;
  }

  public void setBrokerQueryPort(String port) {
    if (port != null && !port.isEmpty()) {
      brokerQueryPort = port;
    }
  }

  public void setControllerPort(String port) {
    if (port != null && !port.isEmpty()) {
      controllerPort = port;
    }
  }

  public void setServerAdminPort(String port) {
    if (port != null && !port.isEmpty()) {
      serverAdminPort = port;
    }
  }

  public String getControllerUrl() {
    return String.format("http://%s:%s", DEFAULT_HOST, controllerPort);
  }

  public String getBrokerUrl() {
    return String.format("http://%s:%s", DEFAULT_HOST, brokerQueryPort);
  }

  public String getDefaultHost() {
    return DEFAULT_HOST;
  }

  public String getKafkaPort() {
    return KAFKA_PORT;
  }

  public String getServerAdminUrl() {
    return String.format("http://%s:%s", DEFAULT_HOST, serverAdminPort);
  }

}
