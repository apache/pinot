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
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.utils.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "DeleteTable", mixinStandardHelpOptions = true)
public class DeleteTableCommand extends AbstractDatabaseBaseAdminCommand {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTableCommand.class);

  @CommandLine.Option(names = {"-tableName"}, required = true, description = "Name of the table to delete.")
  private String _tableName;

  @CommandLine.Option(names = {"-type"}, required = false, description = "realtime|offline.")
  private String _type;

  @CommandLine.Option(names = {"-retention"}, required = false, description = "Retention period for the table "
      + "segments (e.g. 12h, 3d); If not set, the retention period will default to the first config that's not null: "
      + "the cluster setting, then '7d'. Using 0d or -1d will instantly delete segments without retention.")
  private String _retention;

  private String _controllerAddress;

  private AuthProvider _authProvider;

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
    return (getName() + " -tableName " + _tableName + " -type " + _type + " -retention " + _retention
        + super.toString());
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

  @Override
  public boolean execute()
      throws Exception {
    if (_controllerHost == null) {
      _controllerHost = NetUtils.getHostAddress();
    }

    if (!_exec) {
      LOGGER.warn("Dry Running Command: {}", toString());
      LOGGER.warn("Use the -exec option to actually execute the command.");
      return true;
    }

    LOGGER.info("Executing command: {}", toString());
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      fileUploadDownloadClient.getHttpClient().sendDeleteRequest(
          FileUploadDownloadClient.getDeleteTableURI(_controllerProtocol, _controllerHost,
              Integer.parseInt(_controllerPort), _tableName, _type, _retention), getHeadersAsMap(),
          AuthProviderUtils.makeAuthProvider(_authProvider, _authTokenUrl, _authToken, _user, _password));
    } catch (Exception e) {
      LOGGER.error("Got Exception while deleting Pinot Table: {}", _tableName, e);
      return false;
    }
    return true;
  }
}
