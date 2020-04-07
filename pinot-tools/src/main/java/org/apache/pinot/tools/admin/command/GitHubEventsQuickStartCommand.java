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

import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.GitHubEventsQuickstart;
import org.kohsuke.args4j.Option;

/**
 * Command to run GitHubEventsQuickStart
 */
public class GitHubEventsQuickStartCommand extends AbstractBaseAdminCommand implements Command {

  @Option(name = "-personalAccessToken", required = true, metaVar = "<String>", usage = "GitHub personal access token.")
  private String _personalAccessToken;

  @Option(name = "-help", help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  public void setPersonalAccessToken(String personalAccessToken) {
    _personalAccessToken = personalAccessToken;
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "GitHubEventsQuickStart";
  }

  @Override
  public String toString() {
    return ("GitHubEventsQuickStart -personalAccessToken " + _personalAccessToken);
  }

  @Override
  public void cleanup() {
  }

  @Override
  public String description() {
    return "Runs the GitHubEventsQuickstart";
  }

  @Override
  public boolean execute()
      throws Exception {
    PluginManager.get().init();
    new GitHubEventsQuickstart().execute(_personalAccessToken);
    return true;
  }
}
