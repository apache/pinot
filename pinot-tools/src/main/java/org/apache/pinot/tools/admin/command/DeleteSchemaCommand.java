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
import org.apache.pinot.spi.utils.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "DeleteSchema", mixinStandardHelpOptions = true)
public class DeleteSchemaCommand extends AbstractDatabaseBaseAdminCommand {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteSchemaCommand.class);

  @CommandLine.Option(names = {"-schemaName"}, required = true, description = "Schema name.")
  private String _schemaName = null;

  @Override
  public String description() {
    return "Delete schema specified via name";
  }

  @Override
  public String getName() {
    return "DeleteSchema";
  }

  @Override
  public String toString() {
    return (getName() + " -schemaName " + _schemaName + super.toString());
  }

  @Override
  public void cleanup() {
  }

  public DeleteSchemaCommand setSchemaName(String schemaName) {
    _schemaName = schemaName;
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
          FileUploadDownloadClient.getDeleteSchemaURI(_controllerProtocol, _controllerHost,
              Integer.parseInt(_controllerPort), _schemaName), getHeadersAsMap(),
          AuthProviderUtils.makeAuthProvider(_authProvider, _authTokenUrl, _authToken, _user, _password));
    } catch (Exception e) {
      LOGGER.error("Got Exception while deleting Pinot Schema: {}", _schemaName, e);
      return false;
    }
    return true;
  }
}
