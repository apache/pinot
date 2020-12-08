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

import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.BootstrapTableTool;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
public class BootstrapTableCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(BootstrapTableCommand.class.getName());

  @Option(name = "-controllerHost", required = false, metaVar = "<String>", usage = "host name for controller.")
  private String _controllerHost;

  @Option(name = "-controllerPort", required = false, metaVar = "<int>", usage = "http port for broker.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @Option(name = "-dir", required = false, aliases = {"-d", "-directory"}, metaVar = "<String>", usage = "The directory contains all the configs and data to bootstrap a table")
  private String _dir;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

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
    return new BootstrapTableTool(_controllerHost, Integer.parseInt(_controllerPort), _dir).execute();
  }
}
