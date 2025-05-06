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
import java.io.FileNotFoundException;
import org.apache.pinot.common.utils.LogicalTableUtils;
import org.apache.pinot.spi.data.LogicalTable;
import org.apache.pinot.spi.utils.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(name = "AddLogicalTable", mixinStandardHelpOptions = true)
public class AddLogicalTableCommand extends AbstractDatabaseBaseAdminCommand {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddLogicalTableCommand.class);

  @CommandLine.Option(names = {"-logicalTableConfig"}, required = true,
      description = "Path to logical table config file.")
  private String _logicalTableConfigFile = null;

  @Override
  public String description() {
    return "Add logical table specified in the logical table config file to the controller";
  }

  @Override
  public String getName() {
    return "AddLogicalTable";
  }

  @Override
  public String toString() {
    return getName() + " -logicalTableConfig " + _logicalTableConfigFile + super.toString();
  }

  @Override
  public void cleanup() {
  }

  public AddLogicalTableCommand setLogicalTableConfigFile(String logicalTableConfigFile) {
    _logicalTableConfigFile = logicalTableConfigFile;
    return this;
  }

  @Override
  public boolean execute() throws Exception {
    if (_controllerHost == null) {
      _controllerHost = NetUtils.getHostAddress();
    }

    if (!_exec) {
      LOGGER.warn("Dry Running Command: {}", toString());
      LOGGER.warn("Use the -exec option to actually execute the command.");
      return true;
    }

    File logicalTableConfigFile = new File(_logicalTableConfigFile);
    LOGGER.info("Executing command: {}", toString());
    if (!logicalTableConfigFile.exists()) {
      throw new FileNotFoundException("File does not exist: " + _logicalTableConfigFile);
    }

    LogicalTable logicalTable = LogicalTable.fromFile(logicalTableConfigFile);
    boolean success = LogicalTableUtils.postTable(_controllerHost, Integer.parseInt(_controllerPort), logicalTable);
    if (!success) {
      LOGGER.error("Failed to upload Logical Table: {}", logicalTable.getTableName());
      return false;
    }
    return true;
  }
}
