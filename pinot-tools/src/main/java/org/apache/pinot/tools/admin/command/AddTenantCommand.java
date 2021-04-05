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

import org.apache.pinot.common.utils.NetUtil;
import org.apache.pinot.controller.helix.ControllerRequestURLBuilder;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.config.tenant.TenantRole;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AddTenantCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddTenantCommand.class);

  @Option(name = "-controllerHost", required = false, metaVar = "<String>", usage = "host name for controller.")
  private String _controllerHost;

  @Option(name = "-controllerPort", required = false, metaVar = "<int>", usage = "Port number to start the controller at.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @Option(name = "-controllerProtocol", required = false, metaVar = "<String>", usage = "protocol for controller.")
  private String _controllerProtocol = CommonConstants.HTTP_PROTOCOL;

  @Option(name = "-name", required = true, metaVar = "<string>", usage = "Name of the tenant to be created")
  private String _name;

  @Option(name = "-role", required = true, metaVar = "<BROKER/SERVER>", usage = "Tenant role (broker/server).")
  private TenantRole _role;

  @Option(name = "-instanceCount", required = true, metaVar = "<int>", usage = "Number of instances.")
  private int _instanceCount;

  @Option(name = "-offlineInstanceCount", required = true, metaVar = "<int>", usage = "Number of offline instances.")
  private int _offlineInstanceCount;

  @Option(name = "-realTimeInstanceCount", required = true, metaVar = "<int>", usage = "Number of realtime instances.")
  private int _realtimeInstanceCount;

  @Option(name = "-exec", required = false, metaVar = "<boolean>", usage = "Execute the command.")
  private boolean _exec;

  @Option(name = "-user", required = false, metaVar = "<String>", usage = "Username for basic auth.")
  private String _user;

  @Option(name = "-password", required = false, metaVar = "<String>", usage = "Password for basic auth.")
  private String _password;

  @Option(name = "-authToken", required = false, metaVar = "<String>", usage = "Http auth token.")
  private String _authToken;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  private String _controllerAddress;

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

  public AddTenantCommand setAuthToken(String authToken) {
    _authToken = authToken;
    return this;
  }

  public AddTenantCommand setExecute(boolean exec) {
    _exec = exec;
    return this;
  }

  @Override
  public boolean execute()
      throws Exception {
    if (_controllerAddress == null) {
      if (_controllerHost == null) {
        _controllerHost = NetUtil.getHostAddress();
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
            tenant.toJsonString(), makeAuthHeader(makeAuthToken(_authToken, _user, _password)));

    LOGGER.info(res);
    System.out.print(res);
    return true;
  }

  @Override
  public String getName() {
    return "AddTenant";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String toString() {
    String retString =
        ("AddTenant -controllerProtocol " + _controllerProtocol + " -controllerHost " + _controllerHost
            + " -controllerPort " + _controllerPort + " -name " + _name + " -role " + _role + " -instanceCount "
            + _instanceCount + " -offlineInstanceCount " + _offlineInstanceCount + " -realTimeInstanceCount "
            + _realtimeInstanceCount);

    return ((_exec) ? (retString + " -exec") : retString);
  }

  @Override
  public String description() {
    return "Add tenant as per the specification provided.";
  }
}
