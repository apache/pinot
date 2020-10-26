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
package org.apache.pinot.tools.service;

import static org.apache.pinot.tools.utils.PinotConfigUtils.getAvailablePort;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.pinot.broker.broker.helix.HelixBrokerStarter;
import org.apache.pinot.common.utils.NetUtil;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.ControllerStarter;
import org.apache.pinot.minion.MinionStarter;
import org.apache.pinot.server.starter.helix.HelixServerStarter;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.services.ServiceStartable;
import org.apache.pinot.tools.service.api.resources.PinotInstanceStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;


/**
 * PinotServiceManager is a user entry point to start Pinot instances in one process.
 *
 */
public class PinotServiceManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotServiceManager.class);

  private final Map<String, ServiceStartable> _runningInstanceMap = new ConcurrentHashMap<>();

  private final String _zkAddress;
  private final String _clusterName;
  private final int _port;
  private final String _instanceId;
  private PinotServiceManagerAdminApiApplication _pinotServiceManagerAdminApplication;
  private boolean _isStarted = false;

  public PinotServiceManager(String zkAddress, String clusterName) {
    this(zkAddress, clusterName, 0);
  }

  public PinotServiceManager(String zkAddress, String clusterName, int port) {
    this(zkAddress, clusterName, null, port);
  }

  public PinotServiceManager(String zkAddress, String clusterName, String hostname, int port) {
    _zkAddress = zkAddress;
    _clusterName = clusterName;
    if (port == 0) {
      port = getAvailablePort();
    }
    _port = port;
    if (hostname == null || hostname.isEmpty()) {
      hostname = NetUtil.getHostnameOrAddress();
    }
    _instanceId = String.format("ServiceManager_%s_%d", hostname, port);
  }

  public static void main(String[] args) {
    PinotServiceManager pinotServiceManager = new PinotServiceManager("localhost:2181", "pinot-demo", 8085);
    pinotServiceManager.start();
  }

  public String startRole(ServiceRole role, Map<String, Object> properties)
      throws Exception {
    switch (role) {
      case CONTROLLER:
        return startController(new ControllerConf(properties));
      case BROKER:
        return startBroker(new PinotConfiguration(properties));
      case SERVER:
        return startServer(new PinotConfiguration(properties));
      case MINION:
        return startMinion(new PinotConfiguration(properties));
    }
    return null;
  }

  public String startController(ControllerConf controllerConf)
      throws Exception {
    LOGGER.info("Trying to start Pinot Controller...");
    if (controllerConf.getHelixClusterName() == null) {
      controllerConf.setHelixClusterName(_clusterName);
    }
    try {
      if (controllerConf.getZkStr() == null) {
        controllerConf.setZkStr(_zkAddress);
      }
    } catch (Exception e) {
      controllerConf.setZkStr(_zkAddress);
    }
    ControllerStarter controllerStarter = new ControllerStarter(controllerConf);
    controllerStarter.start();
    String instanceId = controllerStarter.getInstanceId();
    _runningInstanceMap.put(instanceId, controllerStarter);
    LOGGER.info("Pinot Controller instance [{}] is Started...", instanceId);
    return instanceId;
  }

  public String startBroker(PinotConfiguration brokerConf)
      throws Exception {
    LOGGER.info("Trying to start Pinot Broker...");
    String brokerHost = brokerConf.getProperty("broker.host");
    HelixBrokerStarter brokerStarter;
    try {
      brokerStarter = new HelixBrokerStarter(brokerConf, _clusterName, _zkAddress, brokerHost);
    } catch (Exception e) {
      LOGGER.error("Failed to initialize Pinot Broker Starter", e);
      throw e;
    }
    try {
      brokerStarter.start();
    } catch (Exception e) {
      LOGGER.error("Failed to start Pinot Broker", e);
      throw e;
    }
    String instanceId = brokerStarter.getInstanceId();
    _runningInstanceMap.put(instanceId, brokerStarter);
    LOGGER.info("Pinot Broker instance [{}] is Started...", instanceId);
    return instanceId;
  }

  public String startServer(PinotConfiguration serverConf)
      throws Exception {
    LOGGER.info("Trying to start Pinot Server...");
    HelixServerStarter serverStarter = new HelixServerStarter(_clusterName, _zkAddress, serverConf);
    serverStarter.start();

    String instanceId = serverStarter.getInstanceId();
    _runningInstanceMap.put(instanceId, serverStarter);
    LOGGER.info("Pinot Server instance [{}] is Started...", instanceId);
    return instanceId;
  }

  public String startMinion(PinotConfiguration minionConf)
      throws Exception {
    LOGGER.info("Trying to start Pinot Minion...");
    MinionStarter minionStarter = new MinionStarter(_clusterName, _zkAddress, minionConf);
    minionStarter.start();

    String instanceId = minionStarter.getInstanceId();
    _runningInstanceMap.put(instanceId, minionStarter);
    LOGGER.info("Pinot Minion instance [{}] is Started...", instanceId);
    return instanceId;
  }

  public boolean stopPinotInstance(ServiceStartable instance) {
    if (instance == null) {
      return false;
    }
    synchronized (instance) {
      ServiceRole role = instance.getServiceRole();
      String instanceId = instance.getInstanceId();
      LOGGER.info("Trying to stop Pinot [{}] Instance [{}] ...", role, instanceId);
      instance.stop();
      LOGGER.info("Pinot [{}] Instance [{}] is Stopped...", role, instanceId);
      _runningInstanceMap.remove(instanceId);
      return true;
    }
  }

  public void start() {
    LOGGER.info("Registering service status handler");
    ServiceStatus.setServiceStatusCallback(_instanceId, new PinotServiceManagerStatusCallback(this));

    if (_port < 0) {
      LOGGER.info("Skip Starting Pinot Service Manager admin application");
    } else {
      LOGGER.info("Starting Pinot Service Manager admin application on port: {}", _port);
      _pinotServiceManagerAdminApplication = new PinotServiceManagerAdminApiApplication(this);
      _pinotServiceManagerAdminApplication.start(_port);
    }
    _isStarted = true;
  }

  public void stop() {
    LOGGER.info("Shutting down Pinot Service Manager admin application...");
    _pinotServiceManagerAdminApplication.stop();
    LOGGER.info("Deregistering service status handler");
    ServiceStatus.removeServiceStatusCallback(_instanceId);
  }

  public boolean isStarted() {
    return _isStarted;
  }

  public String getInstanceId() {
    return _instanceId;
  }

  public int getServicePort() {
    return _port;
  }

  public PinotInstanceStatus getInstanceStatus(String instanceName) {
    ServiceStartable serviceStartable = _runningInstanceMap.get(instanceName);
    if (serviceStartable == null) {
      return null;
    }
    PinotInstanceStatus status =
        new PinotInstanceStatus(serviceStartable.getServiceRole(), serviceStartable.getInstanceId(),
            serviceStartable.getConfig(), ServiceStatus.getServiceStatus(instanceName),
            ServiceStatus.getStatusDescription(instanceName));

    return status;
  }

  public List<String> getRunningInstanceIds() {
    return ImmutableList.copyOf(_runningInstanceMap.keySet());
  }

  public String getZkAddress() {
    return _zkAddress;
  }

  public String getClusterName() {
    return _clusterName;
  }

  public boolean stopPinotInstanceById(String instanceName) {
    return stopPinotInstance(_runningInstanceMap.get(instanceName));
  }
}
