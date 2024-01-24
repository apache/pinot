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

import java.util.Collections;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "DeleteTable")
public class DeleteTableCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTableCommand.class);

  @CommandLine.Option(names = {"-tableName"}, required = true, description = "Name of the table to delete.")
  private String _tableName;

  @CommandLine.Option(names = {"-type"}, required = false, description = "realtime|offline.")
  private String _type;

  @CommandLine.Option(names = {"-retention"}, required = false, description = "Retention period for the table "
      + "segments (e.g. 12h, 3d); If not set, the retention period will default to the first config that's not null: "
      + "the cluster setting, then '7d'. Using 0d or -1d will instantly delete segments without retention.")
  private String _retention;

  @CommandLine.Option(names = {"-controllerHost"}, required = false, description = "host name for controller.")
  private String _controllerHost;

  @CommandLine.Option(names = {"-controllerPort"}, required = false, description = "Port number to start the "
      + "controller at.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @CommandLine.Option(names = {"-controllerProtocol"}, required = false, description = "protocol for controller.")
  private String _controllerProtocol = CommonConstants.HTTP_PROTOCOL;

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

  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, required = false, help = true, description = "Print "
      + "this message.")
  private boolean _help = false;

  private String _controllerAddress;

  private AuthProvider _authProvider;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "DeleteTable";
  }

  @Override
  public String description() {
    return "Delete a Pinot table";
  }

  @Override
  public String toString() {
    String retString =
        ("DeleteTable -tableName " + _tableName + " -type " + _type + " -retention " + _retention
            + " -controllerProtocol " + _controllerProtocol + " -controllerHost " + _controllerHost
            + " -controllerPort " + _controllerPort + " -user " + _user + " -password " + "[hidden]");
    return ((_exec) ? (retString + " -exec") : retString);
  }

  @Override
  public void cleanup() {
  }

  public DeleteTableCommand setTableName(String tableName) {
    _tableName = tableName;
    return this;
  }

  public DeleteTableCommand setType(String type) {
    _type = type;
    return this;
  }

  public DeleteTableCommand setRetention(String retention) {
    _retention = retention;
    return this;
  }

  public DeleteTableCommand setControllerHost(String controllerHost) {
    _controllerHost = controllerHost;
    return this;
  }

  public DeleteTableCommand setControllerPort(String controllerPort) {
    _controllerPort = controllerPort;
    return this;
  }

  public DeleteTableCommand setControllerProtocol(String controllerProtocol) {
    _controllerProtocol = controllerProtocol;
    return this;
  }

  public DeleteTableCommand setUser(String user) {
    _user = user;
    return this;
  }

  public DeleteTableCommand setPassword(String password) {
    _password = password;
    return this;
  }

  public DeleteTableCommand setExecute(boolean exec) {
    _exec = exec;
    return this;
  }

  public DeleteTableCommand setAuthProvider(AuthProvider authProvider) {
    _authProvider = authProvider;
    return this;
  }

  @Override
  public boolean execute()
      throws Exception {
    if (_controllerHost == null) {
      _controllerHost = NetUtils.getHostAddress();
    }

    if (!_exec) {
      LOGGER.warn("Dry Running Command: " + toString());
      LOGGER.warn("Use the -exec option to actually execute the command.");
      return true;
    }

    LOGGER.info("Executing command: " + toString());
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      fileUploadDownloadClient.getHttpClient().sendDeleteRequest(FileUploadDownloadClient
          .getDeleteTableURI(_controllerProtocol, _controllerHost, Integer.parseInt(_controllerPort),
              _tableName, _type, _retention), Collections.emptyMap(), AuthProviderUtils.makeAuthProvider(_authProvider,
          _authTokenUrl, _authToken, _user, _password));
    } catch (Exception e) {
      LOGGER.error("Got Exception while deleting Pinot Table: " + _tableName, e);
      return false;
    }
    return true;
  }
}
