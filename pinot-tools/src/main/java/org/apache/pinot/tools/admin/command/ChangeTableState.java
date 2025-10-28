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

import java.util.Locale;
import org.apache.pinot.client.admin.PinotAdminClient;
import org.apache.pinot.client.admin.PinotAdminException;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.tools.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "ChangeTableState", mixinStandardHelpOptions = true)
public class ChangeTableState extends AbstractDatabaseBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeTableState.class);

  @CommandLine.Option(names = {"-tableName"}, required = true, description = "Table name to disable")
  private String _tableName;

  @CommandLine.Option(names = {"-tableType"}, required = true, description = "Table type (OFFLINE|REALTIME)")
  private TableType _tableType;

  @CommandLine.Option(names = {"-state"}, required = true, description = "Change Table State(enable|disable|drop)")
  private String _state;

  @Override
  public boolean execute()
      throws Exception {
    if (_controllerHost == null) {
      _controllerHost = NetUtils.getHostAddress();
    }

    String stateValue = _state.toLowerCase(Locale.ROOT);
    if (!stateValue.equals("enable") && !stateValue.equals("disable") && !stateValue.equals("drop")) {
      throw new IllegalArgumentException(
          "Invalid value for state: " + _state + "\n Value must be one of enable|disable|drop");
    }
    try (PinotAdminClient adminClient = getPinotAdminClient()) {
      switch (stateValue) {
        case "enable":
          adminClient.getTableClient().setTableState(_tableName, _tableType.name(), true);
          break;
        case "disable":
          adminClient.getTableClient().setTableState(_tableName, _tableType.name(), false);
          break;
        case "drop":
          adminClient.getTableClient().deleteTable(_tableName, _tableType.name(), null, null);
          break;
        default:
          throw new IllegalArgumentException("Unsupported state: " + _state);
      }
    } catch (PinotAdminException e) {
      LOGGER.error("Failed to change table state for {} ({})", _tableName, _tableType, e);
      return false;
    }
    return true;
  }

  @Override
  public String description() {
    return "Change the state (enable|disable|drop) of Pinot table";
  }

  @Override
  public String getName() {
    return "ChangeTableState";
  }

  @Override
  public String toString() {
    return ("ChangeTableState -tableName " + _tableName + " -tableType " + _tableType + " -state " + _state)
        + super.toString();
  }

  @Override
  public void cleanup() {
  }
}
