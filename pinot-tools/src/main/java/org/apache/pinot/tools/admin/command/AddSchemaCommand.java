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
import java.net.URI;
import java.util.Collections;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


@CommandLine.Command(name = "AddSchema", mixinStandardHelpOptions = true)
public class AddSchemaCommand extends AbstractDatabaseBaseAdminCommand {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddSchemaCommand.class);

  @CommandLine.Option(names = {"-schemaFile"}, required = true, description = "Path to schema file.")
  private String _schemaFile = null;

  @CommandLine.Option(names = {"-override"}, required = false, description = "Whether to override the schema if the "
      + "schema exists.")
  private boolean _override = false;

  @CommandLine.Option(names = {"-force"}, required = false, description = "Whether to force overriding the schema if "
      + "the schema exists.")
  private boolean _force = false;

  @Override
  public String description() {
    return "Add schema specified in the schema file to the controller";
  }

  @Override
  public String getName() {
    return "AddSchema";
  }

  @Override
  public String toString() {
    String retString = (getName() + " -schemaFile " + _schemaFile + " -override " + _override + " _force " + _force
        + super.toString());

    return retString;
  }

  @Override
  public void cleanup() {
  }

  public AddSchemaCommand setSchemaFilePath(String schemaFilePath) {
    _schemaFile = schemaFilePath;
    return this;
  }

  public AddSchemaCommand setOverride(boolean override) {
    _override = override;
    return this;
  }

  public AddSchemaCommand setForce(boolean force) {
    _force = force;
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

    File schemaFile = new File(_schemaFile);
    LOGGER.info("Executing command: {}", toString());
    if (!schemaFile.exists()) {
      throw new FileNotFoundException("file does not exist, + " + _schemaFile);
    }

    Schema schema = Schema.fromFile(schemaFile);
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      URI schemaURI = FileUploadDownloadClient.getUploadSchemaURI(_controllerProtocol, _controllerHost,
          Integer.parseInt(_controllerPort));
      schemaURI = new URI(schemaURI + "?override=" + _override + "&force=" + _force);
      fileUploadDownloadClient.addSchema(schemaURI, schema.getSchemaName(), schemaFile, getHeaders(),
          Collections.emptyList());
    } catch (Exception e) {
      LOGGER.error("Got Exception to upload Pinot Schema: {}", schema.getSchemaName(), e);
      return false;
    }
    return true;
  }
}
