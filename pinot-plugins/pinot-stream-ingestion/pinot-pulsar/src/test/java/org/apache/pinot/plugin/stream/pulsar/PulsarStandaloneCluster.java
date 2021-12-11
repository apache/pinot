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

import java.io.File;
import java.io.FileInputStream;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.PulsarStandalone;
import org.apache.pulsar.PulsarStandaloneBuilder;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PulsarStandaloneCluster {
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarStandaloneCluster.class);

  public static final String DEFAULT_STANDALONE_CONF = "standalone.properties";
  public static final String DEFAULT_ZK_DIR = "pulsar-zk";
  public static final String DEFAULT_BK_DIR = "pulsar-bookeeper";

  private Integer _brokerPort;
  private Integer _adminPort;
  private String _zkDir;
  private String _bkDir;

  private PulsarStandalone _pulsarStandalone;
  private File _tempDir;

  public void setBrokerPort(Integer brokerPort) {
    _brokerPort = brokerPort;
  }

  public void setAdminPort(Integer adminPort) {
    _adminPort = adminPort;
  }

  public void setZkDir(String zkDir) {
    _zkDir = zkDir;
  }

  public void setBkDir(String bkDir) {
    _bkDir = bkDir;
  }

  public Integer getBrokerPort() {
    return _brokerPort;
  }

  public Integer getAdminPort() {
    return _adminPort;
  }

  public void start()
      throws Exception {
    final File clusterConfigFile = new File(getClass().getClassLoader().getResource(DEFAULT_STANDALONE_CONF).toURI());

    String zkDir = StringUtils.isBlank(_zkDir) ? DEFAULT_ZK_DIR : _zkDir;
    String bkDir = StringUtils.isBlank(_bkDir) ? DEFAULT_BK_DIR : _bkDir;
    _tempDir = FileUtils.getTempDirectory();
    File zkDirFile = new File(_tempDir, zkDir);
    File bkDirFile = new File(_tempDir, bkDir);
    zkDirFile.mkdirs();
    bkDirFile.mkdirs();

    ServiceConfiguration config =
        PulsarConfigurationLoader.create((new FileInputStream(clusterConfigFile)), ServiceConfiguration.class);
    config.setManagedLedgerDefaultEnsembleSize(1);
    config.setManagedLedgerDefaultWriteQuorum(1);
    config.setManagedLedgerDefaultAckQuorum(1);
    String zkServers = "127.0.0.1";
    config.setAdvertisedAddress("localhost");

    _pulsarStandalone = PulsarStandaloneBuilder.instance().withConfig(config).withNoStreamStorage(true).build();
    _pulsarStandalone.setZkDir(zkDirFile.getAbsolutePath());
    _pulsarStandalone.setBkDir(bkDirFile.getAbsolutePath());

    if (config.getZookeeperServers() != null) {
      _pulsarStandalone.setZkPort(Integer.parseInt(config.getZookeeperServers().split(":")[1]));
    }

    config.setZookeeperServers(zkServers + ":" + _pulsarStandalone.getZkPort());
    config.setConfigurationStoreServers(zkServers + ":" + _pulsarStandalone.getZkPort());

    config.setRunningStandalone(true);

    if (_brokerPort != null) {
      config.setBrokerServicePort(Optional.of(_brokerPort));
    } else {
      _brokerPort = config.getBrokerServicePort().get();
    }

    if (_adminPort != null) {
      config.setWebServicePort(Optional.of(_adminPort));
    } else {
      _adminPort = config.getWebServicePort().get();
    }

    _pulsarStandalone.setConfigFile(clusterConfigFile.getAbsolutePath());
    _pulsarStandalone.setConfig(config);

    _pulsarStandalone.start();
  }

  public void stop() {
    try {
      _pulsarStandalone.close();
      _tempDir.delete();
    } catch (Exception e) {
      LOGGER.warn("Failed to stop embedded pulsar and zookeeper", e);
    }
  }
}
