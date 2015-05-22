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

import org.kohsuke.args4j.Option;

import com.linkedin.pinot.core.data.readers.FileFormat;


public class CreateTenant extends AbstractBaseCommand implements Command {

  @Option(name = "controllerAddress", required = true, metaVar = "<http>",
      usage = "Http address of Controller (with port).")
  private String controllerAddress = null;

  @Option(name = "-name", required = true, metaVar = "<string>", usage = "Name of the tenant to be created")
  private String name;

  @Option(name = "-role", required = false, metaVar = "<AVRO/CSV/JSON>", usage = "Input data format.")
  private FileFormat role = FileFormat.AVRO;

  @Option(name = "-resourceName", required = true, metaVar = "<string>", usage = "Name of the resource.")
  private String numberOfInstances;

  @Option(name = "-segmentName", required = false, metaVar = "<string>", usage = "Name of the segment.")
  private String offlineInstances;

  @Option(name = "-schemaFile", required = false, metaVar = "<string>", usage = "File containing schema for data.")
  private String realtimeInstances;

  @Option(name = "-help", required = false, help = true, usage = "Print this message.")
  private boolean help = false;

  @Override
  public boolean execute() throws Exception {

    return false;
  }

  @Override
  public boolean getHelp() {
    return help;
  }

}
