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
package org.apache.pinot.tools.admin.command;

import java.io.File;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.services.ServiceRole;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.service.PinotServiceManager;
import org.apache.pinot.tools.utils.PinotConfigUtils;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.PINOT_SERVICE_ROLE;


/**
 * Starts services in the following order, and returns false on any startup failure:
 *
 * <p><ol>
 * <li>{@link PinotServiceManager}</li>
 * <li>Bootstrap services in role {@link ServiceRole#CONTROLLER}</li>
 * <li>All remaining bootstrap services in parallel</li>
 * </ol>
 */
public class StartServiceManagerCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartServiceManagerCommand.class);
  private static final long startTick = System.nanoTime();
  private static final String[] BOOTSTRAP_SERVICES = new String[]{"CONTROLLER", "BROKER", "SERVER"};
  // multiple instances allowed per role for testing many minions
  private final List<Entry<ServiceRole, Map<String, Object>>> _bootstrapConfigurations = new ArrayList<>();

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help;
  @Option(name = "-zkAddress", required = false, metaVar = "<http>", usage = "Http address of Zookeeper.", forbids = {"-bootstrapConfigPaths", "-bootstrapServices"})
  private String _zkAddress = DEFAULT_ZK_ADDRESS;
  @Option(name = "-clusterName", required = false, metaVar = "<String>", usage = "Pinot cluster name.", forbids = {"-bootstrapConfigPaths", "-bootstrapServices"})
  private String _clusterName = DEFAULT_CLUSTER_NAME;
  @Option(name = "-port", required = false, metaVar = "<int>", usage = "Pinot service manager admin port, -1 means disable, 0 means a random available port.", forbids = {"-bootstrapConfigPaths", "-bootstrapServices"})
  private int _port = -1;
  @Option(name = "-bootstrapConfigPaths", handler = StringArrayOptionHandler.class, required = false, usage = "A list of Pinot service config file paths. Each config file requires an extra config: 'pinot.service.role' to indicate which service to start.", forbids = {"-zkAddress", "-clusterName", "-port", "-bootstrapServices"})
  private String[] _bootstrapConfigPaths;
  @Option(name = "-bootstrapServices", handler = StringArrayOptionHandler.class, required = false, usage = "A list of Pinot service roles to start with default config. E.g. CONTROLLER/BROKER/SERVER", forbids = {"-zkAddress", "-clusterName", "-port", "-bootstrapConfigPaths"})
  private String[] _bootstrapServices = BOOTSTRAP_SERVICES;

  private PinotServiceManager _pinotServiceManager;

  public String getZkAddress() {
    return _zkAddress;
  }

  public StartServiceManagerCommand setZkAddress(String zkAddress) {
    _zkAddress = zkAddress;
    return this;
  }

  public String getClusterName() {
    return _clusterName;
  }

  public StartServiceManagerCommand setClusterName(String clusterName) {
    _clusterName = clusterName;
    return this;
  }

  public int getPort() {
    return _port;
  }

  public StartServiceManagerCommand setPort(int port) {
    _port = port;
    return this;
  }

  public String[] getBootstrapConfigPaths() {
    return _bootstrapConfigPaths;
  }

  public StartServiceManagerCommand setBootstrapConfigPaths(String[] bootstrapConfigPaths) {
    _bootstrapConfigPaths = bootstrapConfigPaths;
    return this;
  }

  public String[] getBootstrapServices() {
    return _bootstrapServices;
  }

  public StartServiceManagerCommand setBootstrapServices(String[] bootstrapServices) {
    _bootstrapServices = bootstrapServices;
    return this;
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  public void setHelp(boolean help) {
    _help = help;
  }

  @Override
  public String getName() {
    return "StartPinotService";
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder()
        .append("StartServiceManager -clusterName " + _clusterName + " -zkAddress " + _zkAddress + " -port " + _port);
    if (_bootstrapConfigPaths != null) {
      sb.append(" -bootstrapConfigPaths " + Arrays.toString(_bootstrapConfigPaths));
    } else if (_bootstrapServices != null) {
      sb.append(" -bootstrapServices " + Arrays.toString(_bootstrapServices));
    }

    return sb.toString();
  }

  @Override
  public void cleanup() {
    if (_pinotServiceManager != null) {
      _pinotServiceManager.stopAll();
    }
  }

  @Override
  public String description() {
    return "Start the Pinot Service Process at the specified port.";
  }

  @Override
  public boolean execute()
      throws Exception {
    try {
      LOGGER.info("Executing command: " + toString());
      if (!startPinotService("SERVICE_MANAGER", this::startServiceManager)) {
        return false;
      }

      if (_bootstrapConfigPaths != null) {
        for (String configPath : _bootstrapConfigPaths) {
          Map<String, Object> config = readConfigFromFile(configPath);
          ServiceRole role = ServiceRole.valueOf(config.get(PINOT_SERVICE_ROLE).toString());
          addBootstrapService(role, config);
        }
      } else if (_bootstrapServices != null) {
        for (String service : _bootstrapServices) {
          ServiceRole role = ServiceRole.valueOf(service.toUpperCase());
          Map<String, Object> config = getDefaultConfig(role);
          addBootstrapService(role, config);
        }
      }

      if (!startBootstrapServices()) {
        return false;
      }
      String pidFile = ".pinotAdminService-" + System.currentTimeMillis() + ".pid";
      savePID(System.getProperty("java.io.tmpdir") + File.separator + pidFile);
      return true;
    } catch (Exception e) {
      LOGGER.error("Caught exception while starting pinot service, exiting.", e);
      System.exit(-1);
      return false;
    }
  }

  private String startServiceManager() {
    _pinotServiceManager = new PinotServiceManager(_zkAddress, _clusterName, _port);
    _pinotServiceManager.start();
    return _pinotServiceManager.getInstanceId();
  }

  private Map<String, Object> getDefaultConfig(ServiceRole serviceRole)
      throws SocketException, UnknownHostException {
    switch (serviceRole) {
      case CONTROLLER:
        return PinotConfigUtils.generateControllerConf(_zkAddress, _clusterName, null, DEFAULT_CONTROLLER_PORT, null,
            ControllerConf.ControllerMode.DUAL, true);
      case BROKER:
        return PinotConfigUtils.generateBrokerConf(_clusterName, _zkAddress, null, CommonConstants.Helix.DEFAULT_BROKER_QUERY_PORT);
      case SERVER:
        return PinotConfigUtils.generateServerConf(_clusterName, _zkAddress, null, CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT,
            CommonConstants.Server.DEFAULT_ADMIN_API_PORT, null, null);
      default:
        throw new RuntimeException("No default config found for service role: " + serviceRole);
    }
  }

  /**
   * Starts a controller synchronously unless the cluster already exists. Other services start in parallel.
   */
  private boolean startBootstrapServices() {
    if (_bootstrapConfigurations.isEmpty()) return true;

    List<Entry<ServiceRole, Map<String, Object>>> parallelConfigs = new ArrayList<>();

    // Start controller(s) synchronously so that other services don't fail
    //
    // Note: Technically, we don't need to do this if the cluster already exists, but checking the
    // cluster takes time and clutters logs with errors when it doesn't exist.
    for (Entry<ServiceRole, Map<String, Object>> roleToConfig : _bootstrapConfigurations) {
      if (roleToConfig.getKey() == ServiceRole.CONTROLLER) {
        if (!startPinotService(ServiceRole.CONTROLLER,
            () -> _pinotServiceManager.startRole(ServiceRole.CONTROLLER, roleToConfig.getValue()))) {
          return false;
        }
      } else {
        parallelConfigs.add(roleToConfig);
      }
    }

    return startBootstrapServicesInParallel(_pinotServiceManager, parallelConfigs);
  }

  static boolean startBootstrapServicesInParallel(
      PinotServiceManager pinotServiceManager,
      List<Entry<ServiceRole, Map<String, Object>>> parallelConfigs
  ) {
    if (parallelConfigs.isEmpty()) return true;

    // True is when everything succeeded
    AtomicBoolean failed = new AtomicBoolean(false);

    List<Thread> threads = new ArrayList<>();
    for (Entry<ServiceRole, Map<String, Object>> roleToConfig : parallelConfigs) {
      ServiceRole role = roleToConfig.getKey();
      Map<String, Object> config = roleToConfig.getValue();
      Thread thread = new Thread("Start a Pinot [" + role + "]") {
        @Override public void run() {
          if (!startPinotService(role, () -> pinotServiceManager.startRole(role, config))) {
            failed.set(true);
          }
        }
      };
      threads.add(thread);
      // Unhandled exceptions are likely logged, so we don't need to re-log here
      thread.setUncaughtExceptionHandler((t, e) -> failed.set(true));
      thread.start();
    }

    // Block until service startup completes
    for (Thread thread : threads) {
      try {
        thread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
    return !failed.get();
  }

  private static boolean startPinotService(Object role, Callable<String> serviceStarter) {
    try {
      LOGGER.info("Starting a Pinot [{}] at {}s since launch", role, startOffsetSeconds());
      String instanceId = serviceStarter.call();
      LOGGER.info("Started Pinot [{}] instance [{}] at {}s since launch", role, instanceId, startOffsetSeconds());
    } catch (Exception e) {
      LOGGER.error(String.format("Failed to start a Pinot [%s] at %s since launch", role, startOffsetSeconds()), e);
      return false;
    }
    return true;
  }

  /** Creates millis precision unit of seconds. ex 1.002 */
  private static float startOffsetSeconds() {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTick) / 1000f;
  }

  public StartServiceManagerCommand addBootstrapService(ServiceRole role, Map<String, Object> config) {
    if (role == null) throw new NullPointerException("role == null");
    config.put(PINOT_SERVICE_ROLE, role.toString()); // Ensure config has role key
    _bootstrapConfigurations.add(new SimpleImmutableEntry<>(role, config));
    return this;
  }
}
