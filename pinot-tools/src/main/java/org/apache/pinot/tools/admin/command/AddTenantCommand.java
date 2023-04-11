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

import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.config.tenant.TenantRole;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "AddTenant", description = "Add tenant as per the specification provided.",
    mixinStandardHelpOptions = true)
public class AddTenantCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddTenantCommand.class);

  @CommandLine.Option(names = {"-controllerHost"}, required = false, description = "host name for controller.")
  private String _controllerHost;

  @CommandLine.Option(names = {"-controllerPort"}, required = false,
      description = "Port number to start the controller at.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @CommandLine.Option(names = {"-controllerProtocol"}, required = false, description = "protocol for controller.")
  private String _controllerProtocol = CommonConstants.HTTP_PROTOCOL;

  @CommandLine.Option(names = {"-name"}, required = true, description = "Name of the tenant to be created")
  private String _name;

  @CommandLine.Option(names = {"-role"}, required = true, description = "Tenant role (broker/server).")
  private TenantRole _role;

  @CommandLine.Option(names = {"-instanceCount"}, required = true, description = "Number of instances.")
  private int _instanceCount;

  @CommandLine.Option(names = {"-offlineInstanceCount"}, required = true, description = "Number of offline instances.")
  private int _offlineInstanceCount;

  @CommandLine.Option(names = {"-realTimeInstanceCount"}, required = true,
      description = "Number of realtime instances.")
  private int _realtimeInstanceCount;

  @CommandLine.Option(names = {"-exec"}, required = false, description = "Execute the command.")
  private boolean _exec;

  @CommandLine.Option(names = {"-user"}, required = false, description = "Username for basic auth.")
  private String _user;

  @CommandLine.Option(names = {"-password"}, required = false, description = "Password for basic auth.")
  private String _password;

  @CommandLine.Option(names = {"-authToken"}, required = false, description = "Http auth token.")
  private String _authToken;

  @CommandLine.Option(names = {"-authTokenUrl"}, required = false, description = "Http auth token url.")
  private String _authTokenUrl;

  private String _controllerAddress;

  private AuthProvider _authProvider;

  public AddTenantCommand setControllerUrl(String url) {
    _controllerAddress = url;
    return this;
  }

  public AddTenantCommand setName(String name) {
    _name = name;
    return this;
  }

  public AddTenantCommand setRole(TenantRole role) {
    _role = role;
    return this;
  }

  public AddTenantCommand setInstances(int instances) {
    _instanceCount = instances;
    return this;
  }

  public AddTenantCommand setOffline(int offline) {
    _offlineInstanceCount = offline;
    return this;
  }

  public AddTenantCommand setRealtime(int realtime) {
    _realtimeInstanceCount = realtime;
    return this;
  }

  public AddTenantCommand setUser(String user) {
    _user = user;
    return this;
  }

  public AddTenantCommand setPassword(String password) {
    _password = password;
    return this;
  }

  public AddTenantCommand setExecute(boolean exec) {
    _exec = exec;
    return this;
  }

  public AddTenantCommand setAuthProvider(AuthProvider authProvider) {
    _authProvider = authProvider;
    return this;
  }

  @Override
  public boolean execute()
      throws Exception {
    if (_controllerAddress == null) {
      if (_controllerHost == null) {
        _controllerHost = NetUtils.getHostAddress();
      }
      _controllerAddress = _controllerProtocol + "://" + _controllerHost + ":" + _controllerPort;
    }

    if (!_exec) {
      LOGGER.warn("Dry Running Command: " + toString());
      LOGGER.warn("Use the -exec option to actually execute the command.");
      return true;
    }

    LOGGER.info("Executing command: " + toString());
    Tenant tenant = new Tenant(_role, _name, _instanceCount, _offlineInstanceCount, _realtimeInstanceCount);
    String res = AbstractBaseAdminCommand
        .sendRequest("POST", ControllerRequestURLBuilder.baseUrl(_controllerAddress).forTenantCreate(),
            tenant.toJsonString(), makeAuthHeaders(makeAuthProvider(_authProvider, _authTokenUrl, _authToken, _user,
                _password)));

    LOGGER.info(res);
    System.out.print(res);
    return true;
  }

  @Override
  public String getName() {
    return "AddTenant";
  }

  @Override
  public String toString() {
    String retString = ("AddTenant -controllerProtocol " + _controllerProtocol + " -controllerHost " + _controllerHost
        + " -controllerPort " + _controllerPort + " -name " + _name + " -role " + _role + " -instanceCount "
        + _instanceCount + " -offlineInstanceCount " + _offlineInstanceCount + " -realTimeInstanceCount "
        + _realtimeInstanceCount);

    return ((_exec) ? (retString + " -exec") : retString);
  }
}
