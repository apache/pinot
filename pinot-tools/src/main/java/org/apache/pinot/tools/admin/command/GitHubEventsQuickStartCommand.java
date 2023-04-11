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
import picocli.CommandLine;


/**
 * Command to run GitHubEventsQuickStart
 */
@CommandLine.Command(name = "GitHubEventsQuickStart", description = "Runs the GitHubEventsQuickstart",
    mixinStandardHelpOptions = true)
public class GitHubEventsQuickStartCommand extends AbstractBaseAdminCommand implements Command {

  @CommandLine.Option(names = {"-personalAccessToken"}, required = true, description = "GitHub personal access token.")
  private String _personalAccessToken;

  @CommandLine.Option(names = {"-sourceType"}, defaultValue = "Kafka",
      description = "Stream DataSource to use for ingesting data. Supported values - Kafka,Kinesis")
  private String _sourceType;

  public void setPersonalAccessToken(String personalAccessToken) {
    _personalAccessToken = personalAccessToken;
  }

  public void setSourceType(String sourceType) {
    _sourceType = sourceType;
  }

  @Override
  public String getName() {
    return "GitHubEventsQuickStart";
  }

  @Override
  public String toString() {
    return ("GitHubEventsQuickStart -personalAccessToken " + _personalAccessToken + " -sourceType" + _sourceType);
  }

  @Override
  public void cleanup() {
  }

  @Override
  public boolean execute()
      throws Exception {
    PluginManager.get().init();
    new GitHubEventsQuickstart().setPersonalAccessToken(_personalAccessToken).setSourceType(_sourceType).execute();
    return true;
  }
}
