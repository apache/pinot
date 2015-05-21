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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.codehaus.jackson.map.ObjectMapper;
import org.kohsuke.args4j.Option;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.FileUploadUtils;


public class AddSchemaCommand extends AbstractBaseCommand implements Command {
  @Option(name = "-controllerHost", required = true, metaVar = "<string>", usage = "Hostname for controller.")
  private String controllerHost = null;

  @Option(name = "-controllerPort", required = true, metaVar = "<String>", usage = "Hostname for controller.")
  private String controllerPort;

  @Option(name = "-schemaFilePath", required = true, metaVar = "<string>", usage = "Path to segment directory.")
  private String schemaFilePath = null;

  @Option(name = "-help", required = false, help = true, usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "UploadSchema";
  }

  @Override
  public String toString() {
    String res = null;
    try {
      res = new ObjectMapper().writeValueAsString(this);
    } catch (Exception e) {
      e.printStackTrace();
    }

    return res;
  }

  @Override
  public void cleanup() {

  }

  public AddSchemaCommand setControllerHost(String controllerHost) {
    this.controllerHost = controllerHost;
    return this;
  }

  public AddSchemaCommand setControllerPort(String controllerPort) {
    this.controllerPort = controllerPort;
    return this;
  }

  public AddSchemaCommand setSchemaFilePath(String schemaFilePath) {
    this.schemaFilePath = schemaFilePath;
    return this;
  }

  @Override
  public boolean execute() throws Exception {
    File schemaFile = new File(schemaFilePath);

    if (!schemaFile.exists()) {
      throw new FileNotFoundException("file does not exist, + " + schemaFilePath);
    }

    Schema s = Schema.fromFile(schemaFile);

    FileUploadUtils.sendFile(controllerHost, controllerPort, "schemas", s.getSchemaName(), new FileInputStream(
        schemaFile), schemaFile.length());

    return true;
  }
}
