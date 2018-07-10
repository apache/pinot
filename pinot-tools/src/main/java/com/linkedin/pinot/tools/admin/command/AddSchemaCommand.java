/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools.admin.command;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.FileUploadDownloadClient;
import com.linkedin.pinot.common.utils.NetUtil;
import com.linkedin.pinot.tools.Command;
import java.io.File;
import java.io.FileNotFoundException;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AddSchemaCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(AddSchemaCommand.class);

  @Option(name = "-controllerHost", required = false, metaVar = "<String>", usage = "host name for controller.")
  private String _controllerHost;

  @Option(name = "-controllerPort", required = false, metaVar = "<String>", usage = "port name for controller.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @Option(name = "-schemaFile", required = true, metaVar = "<string>", usage = "Path to schema file.")
  private String _schemaFile = null;

  @Option(name = "-exec", required = false, metaVar = "<boolean>", usage = "Execute the command.")
  private boolean _exec;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

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
    String retString =
        ("AddSchema -controllerHost " + _controllerHost + " -controllerPort " + _controllerPort + " -schemaFilePath "
            + _schemaFile);

    return ((_exec) ? (retString + " -exec") : retString);
  }

  @Override
  public void cleanup() {

  }

  public AddSchemaCommand setControllerHost(String controllerHost) {
    _controllerHost = controllerHost;
    return this;
  }

  public AddSchemaCommand setControllerPort(String controllerPort) {
    _controllerPort = controllerPort;
    return this;
  }

  public AddSchemaCommand setSchemaFilePath(String schemaFilePath) {
    _schemaFile = schemaFilePath;
    return this;
  }

  public AddSchemaCommand setExecute(boolean exec) {
    _exec = exec;
    return this;
  }

  @Override
  public boolean execute() throws Exception {
    if (_controllerHost == null) {
      _controllerHost = NetUtil.getHostAddress();
    }

    if (!_exec) {
      LOGGER.warn("Dry Running Command: " + toString());
      LOGGER.warn("Use the -exec option to actually execute the command.");
      return true;
    }

    File schemaFile = new File(_schemaFile);
    LOGGER.info("Executing command: " + toString());
    if (!schemaFile.exists()) {
      throw new FileNotFoundException("file does not exist, + " + _schemaFile);
    }

    Schema schema = Schema.fromFile(schemaFile);
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      fileUploadDownloadClient.addSchema(
          FileUploadDownloadClient.getUploadSchemaHttpURI(_controllerHost, Integer.parseInt(_controllerPort)),
          schema.getSchemaName(), schemaFile);
    }

    return true;
  }
}
