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

import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.tools.BootstrapTableTool;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * The command to bootstrap a Pinot table from a directory with table schema/config/ingestionJobSpec/raw data files.
 *
 * Sample usage:
 * {@code pinot-admin.sh BootstrapTable -dir <path-to-table-configs-directory> }
 *
 * The directory structure is based on current example conventions:
 * For offline table:
 * ```
 * <table_name>/
 * <table_name>/<table_name>_schema.json
 * <table_name>/<table_name>_offline_table_config.json
 * <table_name>/ingestionJobSpec.yaml
 * <table_name>/rawdata/...
 * ```
 *
 * For realtime table:
 * ```
 * <table_name>/
 * <table_name>/<table_name>_schema.json
 * <table_name>/<table_name>_realtime_table_config.json
 * ```
 *
 * For hybrid table:
 * ```
 * <table_name>/
 * <table_name>/<table_name>_schema.json
 * <table_name>/<table_name>_offline_table_config.json
 * <table_name>/<table_name>_realtime_table_config.json
 * <table_name>/ingestionJobSpec.yaml
 * <table_name>/rawdata/...
 * ```
 */
@CommandLine.Command(name = "BootstrapTable")
public class BootstrapTableCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapTableCommand.class.getName());

  @CommandLine.Option(names = {"-controllerHost"}, required = false, description = "host name for controller.")
  private String _controllerHost;

  @CommandLine.Option(names = {"-controllerPort"}, required = false, description = "http port for broker.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @CommandLine.Option(names = {"-controllerProtocol"}, required = false, description = "protocol for controller.")
  private String _controllerProtocol = CommonConstants.HTTP_PROTOCOL;

  @CommandLine.Option(names = {"-dir", "-d", "-directory"}, required = false,
      description = "The directory contains all the configs and data to bootstrap a table")
  private String _dir;

  @CommandLine.Option(names = {"-user"}, required = false, description = "Username for basic auth.")
  private String _user;

  @CommandLine.Option(names = {"-password"}, required = false, description = "Password for basic auth.")
  private String _password;

  @CommandLine.Option(names = {"-authToken"}, required = false, description = "Http auth token.")
  private String _authToken;

  @CommandLine.Option(names = {"-authTokenUrl"}, required = false, description = "Http auth token url.")
  private String _authTokenUrl;

  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, required = false, help = true,
      description = "Print this message.")
  private boolean _help = false;

  private AuthProvider _authProvider;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "BootstrapTable";
  }

  public BootstrapTableCommand setDir(String dir) {
    _dir = dir;
    return this;
  }

  public BootstrapTableCommand setAuthProvider(AuthProvider authProvider) {
    _authProvider = authProvider;
    return this;
  }

  @Override
  public String toString() {
    return ("BootstrapTable -dir " + _dir);
  }

  @Override
  public void cleanup() {
  }

  @Override
  public String description() {
    return "Run Pinot Bootstrap Table.";
  }

  @Override
  public boolean execute()
      throws Exception {
    PluginManager.get().init();
    if (_controllerHost == null) {
      _controllerHost = NetUtils.getHostAddress();
    }
    return new BootstrapTableTool(_controllerProtocol, _controllerHost, Integer.parseInt(_controllerPort), _dir,
        AuthProviderUtils.makeAuthProvider(_authProvider, _authTokenUrl, _authToken, _user, _password)).execute();
  }
}
