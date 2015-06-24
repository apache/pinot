/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.common.utils.FileUploadUtils;
import org.kohsuke.args4j.Option;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;


public class AddSchemaCommand extends AbstractBaseCommand implements Command {

  @Option(name = "-controllerHost", required = false, metaVar = "<string>", usage = "host name for controller.")
  private String _controllerHost = "localhost";

  @Option(name = "-controllerPort", required = false, metaVar = "<string>", usage = "port name for controller.")
  private String _controllerPort = DEFAULT_CONTROLLER_PORT;

  @Option(name = "-schemaFile", required = true, metaVar = "<string>", usage = "Path to schema file.")
  private String _schemaFile = null;

  @Option(name = "-exec", required = false, metaVar = "<boolean>", usage = "Execute the command.")
  private boolean _exec;

  @Option(name = "-help", required = false, help = true, aliases = { "-h", "--h", "--help" },
      usage = "Print this message.")
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
    String retString = ("AddSchema -controllerHost " + _controllerHost + " -controllerPort " + _controllerPort
        + " -schemaFilePath " + _schemaFile);

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
    if (!_exec) {
      System.out.println("Dry Running Command: " + toString());
      System.out.println("Use the -exec option to actually execute the command.");
      return true;
    }

    File schemaFile = new File(_schemaFile);

    if (!schemaFile.exists()) {
      throw new FileNotFoundException("file does not exist, + " + _schemaFile);
    }

    Schema s = Schema.fromFile(schemaFile);

    FileUploadUtils.sendFile(_controllerHost, _controllerPort, "schemas", s.getSchemaName(), new FileInputStream(
        schemaFile), schemaFile.length());

    return true;
  }
}
