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

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.kohsuke.args4j.Option;

import com.linkedin.pinot.controller.helix.ControllerRequestURLBuilder;


/**
 * Class to implement CreateResource command.
 *
 * @author Mayank Shrivastava <mshrivastava@linkedin.com>
 */
public class AddTableCommand extends AbstractBaseCommand implements Command {

  @Option(name = "-filePath", required = true, metaVar = "<string>", usage = "Path to the request.json file")
  private String filePath;

  @Option(name = "-controllerAddress", required = true, metaVar = "<http>",
      usage = "Http address of Controller (with port).")
  private String controllerAddress = null;

  @Option(name = "-help", required = false, help = true, usage = "Print this message.")
  private boolean _help = false;

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "CreateResource";
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

  public AddTableCommand setFilePath(String filePath) {
    this.filePath = filePath;
    return this;
  }

  public AddTableCommand setControllerAddress(String controllerAddress) {
    this.controllerAddress = controllerAddress;
    return this;
  }

  @Override
  public boolean execute() throws Exception {
    JsonNode node = new ObjectMapper().readTree(filePath);
    String res =
        AbstractBaseCommand.sendPostRequest(ControllerRequestURLBuilder.baseUrl(controllerAddress).forTableCreate(),
            node.toString());

    System.out.println(res);
    return true;
  }
}
