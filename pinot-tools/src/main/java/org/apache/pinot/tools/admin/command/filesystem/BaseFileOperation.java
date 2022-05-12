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
package org.apache.pinot.tools.admin.command.filesystem;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.admin.command.FileSystemCommand;
import org.apache.pinot.tools.admin.command.QuickstartRunner;
import org.apache.pinot.tools.utils.PinotConfigUtils;
import picocli.CommandLine;


/**
 * Base class for file operation classes
 */
public abstract class BaseFileOperation implements Command {

  @CommandLine.ParentCommand
  protected FileSystemCommand _parent;

  public BaseFileOperation setParent(FileSystemCommand parent) {
    _parent = parent;
    return this;
  }

  protected void initialPinotFS()
      throws Exception {
    String configFile = _parent.getConfigFile();
    Map<String, Object> configs =
        configFile == null ? new HashMap<>() : PinotConfigUtils.readConfigFromFile(configFile);
    PinotFSFactory.init(new PinotConfiguration(configs));
    QuickstartRunner.registerDefaultPinotFS();
  }

  @Override
  public boolean getHelp() {
    return _parent.getHelp();
  }
}
