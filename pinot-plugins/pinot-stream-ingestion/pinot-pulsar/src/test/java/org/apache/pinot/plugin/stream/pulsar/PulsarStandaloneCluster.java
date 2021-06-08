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
package org.apache.pinot.plugin.stream.pulsar;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PulsarStandaloneCluster {
  private static Logger LOGGER = LoggerFactory.getLogger(PulsarStandaloneCluster.class);

  public static final Integer DEFAULT_BROKER_PORT = 6650;
  public static final Integer DEFAULT_ADMIN_PORT = 8080;
  public static final String DEFAULT_DATA_MOUNT_DIRECTORY = "pulsar-data";
  public static final String DEFAULT_CONF_MOUNT_DIRECTORY = "pulsar-conf";
  public static final String DOCKER_CONTAINER_NAME = "pulsar_standalone_pinot";

  private Integer _brokerPort;
  private Integer _adminPort;
  private String _dataMountDirectory;
  private String _confMountDirectory;

  private Process _pulsarCluster;

  public static final String DOCKER_COMMAND =
      "docker run --name " + DOCKER_CONTAINER_NAME + " -p %d:6650 -p %d:8080 " + "  --mount source=pulsardata,target=%s"
          + "  --mount source=pulsarconf,target=%s " + "  apachepulsar/pulsar:2.7.2 bin/pulsar standalone";

  public static final String DOCKER_STOP_COMMAND = "docker stop " + DOCKER_CONTAINER_NAME;
  public static final String DOCKER_REMOVE_COMMAND = "docker rm " + DOCKER_CONTAINER_NAME;

  public PulsarStandaloneCluster() {

  }

  public void setBrokerPort(Integer brokerPort) {
    _brokerPort = brokerPort;
  }

  public void setAdminPort(Integer adminPort) {
    _adminPort = adminPort;
  }

  public Integer getBrokerPort() {
    Integer brokerPort = _brokerPort == null ? DEFAULT_BROKER_PORT : _brokerPort;
    return brokerPort;
  }

  public Integer getAdminPort() {
    Integer adminPort = _adminPort == null ? DEFAULT_ADMIN_PORT : _adminPort;
    return adminPort;
  }

  public void setDataMountDirectory(String dataMountDirectory) {
    _dataMountDirectory = dataMountDirectory;
  }

  public void setConfMountDirectory(String confMountDirectory) {
    _confMountDirectory = confMountDirectory;
  }

  public void start()
      throws Exception {
    Integer brokerPort = _brokerPort == null ? DEFAULT_BROKER_PORT : _brokerPort;
    Integer adminPort = _adminPort == null ? DEFAULT_ADMIN_PORT : _adminPort;
    String dataMountDirectory = _dataMountDirectory == null ? DEFAULT_DATA_MOUNT_DIRECTORY : _dataMountDirectory;
    String confMountDirectory = _confMountDirectory == null ? DEFAULT_CONF_MOUNT_DIRECTORY : _confMountDirectory;

    File tempDir = FileUtils.getTempDirectory();
    File dataDir = new File(tempDir, dataMountDirectory);
    File confDir = new File(tempDir, confMountDirectory);
    dataDir.mkdirs();
    confDir.mkdirs();

    String formattedCommand =
        String.format(DOCKER_COMMAND, brokerPort, adminPort, dataDir.getAbsolutePath(), confDir.getAbsolutePath());

    _pulsarCluster = Runtime.getRuntime().exec(formattedCommand);

    Thread.sleep(30000L);
  }

  public void stop()
      throws Exception {
    if (_pulsarCluster != null && _pulsarCluster.isAlive()) {
      _pulsarCluster.destroy();
      _pulsarCluster = null;
    }
    Process process = Runtime.getRuntime().exec(DOCKER_STOP_COMMAND);

    Thread.sleep(5000);
    try {
      if (process.exitValue() != 0) {
        throw new RuntimeException("Could not stop running docker container " + DOCKER_CONTAINER_NAME);
      }
    } catch (IllegalThreadStateException ignore) {
    }

    process = Runtime.getRuntime().exec(DOCKER_REMOVE_COMMAND);
  }
}
