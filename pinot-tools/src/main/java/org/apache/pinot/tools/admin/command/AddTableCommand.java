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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import org.apache.pinot.spi.config.TableConfigs;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class to implement CreateResource command.
 *
 */
@CommandLine.Command(name = "AddTable", mixinStandardHelpOptions = true)
public class AddTableCommand extends AbstractDatabaseBaseAdminCommand {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddTableCommand.class);

  @CommandLine.Option(names = {"-tableConfigFile", "-tableConf", "-tableConfig", "-filePath"}, description = "Path to"
      + " table config file.")
  private String _tableConfigFile;

  @CommandLine.Option(names = {
      "-offlineTableConfigFile", "-offlineTableConf", "-offlineTableConfig", "-offlineFilePath"
  }, description = "Path to offline table config file.")
  private String _offlineTableConfigFile;

  @CommandLine.Option(names = {
      "-realtimeTableConfigFile", "-realtimeTableConf", "-realtimeTableConfig", "-realtimeFilePath"
  }, description = "Path to realtime table config file.")
  private String _realtimeTableConfigFile;

  @CommandLine.Option(names = {"-schemaFile", "-schemaFileName", "-schema"}, required = false, description = "Path to"
      + " table schema file.")
  private String _schemaFile = null;

  @CommandLine.Option(names = {"-update"}, required = false, description = "Update the existing table instead of "
      + "creating new one")
  private boolean _update = false;

  @CommandLine.Option(names = {"-validationTypesToSkip"}, required = false, description =
      "comma separated list of validation type(s) to skip. supported types: (ALL|TASK|UPSERT)")
  private String _validationTypesToSkip = null;

  private String _controllerAddress;

  @Override
  public String getName() {
    return "AddTable";
  }

  @Override
  public String description() {
    return "Create a Pinot table";
  }

  @Override
  public String toString() {
    String retString =
        (getName() + " -tableConfigFile " + _tableConfigFile + " -offlineTableConfigFile " + _offlineTableConfigFile
            + " -realtimeTableConfigFile " + _realtimeTableConfigFile + " -schemaFile " + _schemaFile
            + super.toString());

    return retString;
  }

  @Override
  public void cleanup() {
  }

  public AddTableCommand setTableConfigFile(String tableConfigFile) {
    _tableConfigFile = tableConfigFile;
    return this;
  }

  public AddTableCommand setOfflineTableConfigFile(String offlineTableConfigFile) {
    _offlineTableConfigFile = offlineTableConfigFile;
    return this;
  }

  public AddTableCommand setRealtimeTableConfigFile(String realtimeTableConfigFile) {
    _realtimeTableConfigFile = realtimeTableConfigFile;
    return this;
  }

  public AddTableCommand setSchemaFile(String schemaFile) {
    _schemaFile = schemaFile;
    return this;
  }

  public String getValidationTypesToSkip() {
    return _validationTypesToSkip;
  }

  public AddTableCommand setValidationTypesToSkip(String validationTypesToSkip) {
    _validationTypesToSkip = validationTypesToSkip;
    return this;
  }

  public boolean sendTableCreationRequest(JsonNode node)
      throws IOException {
    String res = AbstractBaseAdminCommand.sendRequest("POST",
        getTableCreationRequestURL(), node.toString(), getHeaders(),
        makeTrustAllSSLContext());
    LOGGER.info(res);
    return res.contains("successfully added");
  }

  private String getTableCreationRequestURL() {
    String baseURL = ControllerRequestURLBuilder.baseUrl(_controllerAddress).forTableConfigsCreate();
    if (_validationTypesToSkip != null) {
      return String.format(baseURL + "?validationTypesToSkip=%s", _validationTypesToSkip);
    }
    return baseURL;
  }

  public boolean sendTableUpdateRequest(JsonNode node, String tableName)
      throws IOException {
    String res = AbstractBaseAdminCommand.sendRequest("PUT",
        ControllerRequestURLBuilder.baseUrl(_controllerAddress).forTableConfigsUpdate(tableName), node.toString(),
        getHeaders(), makeTrustAllSSLContext());
    LOGGER.info(res);
    return res.contains("TableConfigs updated");
  }

  @Override
  public boolean execute()
      throws Exception {
    if (!_exec) {
      LOGGER.warn("Dry Running Command: {}", toString());
      LOGGER.warn("Use the -exec option to actually execute the command.");
      return true;
    }

    if (_controllerHost == null) {
      _controllerHost = NetUtils.getHostAddress();
    }
    _controllerAddress = _controllerProtocol + "://" + _controllerHost + ":" + _controllerPort;

    LOGGER.info("Executing command: {}", toString());

    String rawTableName = null;
    TableConfig offlineTableConfig = null;
    TableConfig realtimeTableConfig = null;
    if (_tableConfigFile != null) {
      TableConfig tableConfig = attempt(() -> JsonUtils.fileToObject(new File(_tableConfigFile), TableConfig.class),
          "Failed reading table config " + _tableConfigFile);
      rawTableName = TableNameBuilder.extractRawTableName(tableConfig.getTableName());
      if (tableConfig.getTableType() == TableType.OFFLINE) {
        offlineTableConfig = tableConfig;
      } else {
        realtimeTableConfig = tableConfig;
      }
    }

    if (_offlineTableConfigFile != null) {
      offlineTableConfig = attempt(() -> JsonUtils.fileToObject(new File(_offlineTableConfigFile), TableConfig.class),
          "Failed reading offline table config " + _offlineTableConfigFile);
      rawTableName = TableNameBuilder.extractRawTableName(offlineTableConfig.getTableName());
    }
    if (_realtimeTableConfigFile != null) {
      realtimeTableConfig = attempt(() -> JsonUtils.fileToObject(new File(_realtimeTableConfigFile), TableConfig.class),
          "Failed reading realtime table config " + _realtimeTableConfigFile);
      rawTableName = TableNameBuilder.extractRawTableName(realtimeTableConfig.getTableName());
    }

    Preconditions.checkState(rawTableName != null,
        "Must provide at least one of -tableConfigFile, -offlineTableConfigFile, -realtimeTableConfigFile");

    Schema schema = attempt(() -> JsonUtils.fileToObject(new File(_schemaFile), Schema.class),
        "Failed reading schema " + _schemaFile);
    TableConfigs tableConfigs = new TableConfigs(rawTableName, schema, offlineTableConfig, realtimeTableConfig);

    if (_update) {
      return sendTableUpdateRequest(tableConfigs.toJsonNode(), rawTableName);
    } else {
      return sendTableCreationRequest(tableConfigs.toJsonNode());
    }
  }

  private static <T> T attempt(Callable<T> callable, String errorMessage) {
    try {
      return callable.call();
    } catch (Throwable t) {
      LOGGER.error(errorMessage, t);
      throw new IllegalStateException(t);
    }
  }
}
