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
import org.apache.pinot.tools.streams.githubevents.PullRequestMergedEventsStream;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.kohsuke.args4j.Option;


/**
 * Command to stream GitHub events into a kafka topic
 */
public class StreamGitHubEventsCommand extends AbstractBaseAdminCommand implements Command {

  private static final String PULL_REQUEST_MERGED_EVENT_TYPE = "pullRequestMergedEvent";

  @Option(name = "-personalAccessToken", required = true, metaVar = "<String>", usage = "GitHub personal access token.")
  private String _personalAccessToken;

  @Option(name = "-kafkaBrokerList", metaVar = "<String>", usage = "Kafka broker list of the kafka cluster to produce events.")
  private String _kafkaBrokerList = KafkaStarterUtils.DEFAULT_KAFKA_BROKER;

  @Option(name = "-topic", required = true, metaVar = "<String>", usage = "Name of kafka topic to publish events.")
  private String _topic;

  @Option(name = "-eventType", metaVar = "<String>", usage = "Type of GitHub event. Supported types - pullRequestMergedEvent")
  private String _eventType = PULL_REQUEST_MERGED_EVENT_TYPE;

  @Option(name = "-schemaFile", metaVar = "<String>", usage = "Path to schema file. By default uses examples/stream/githubEvents/pullRequestMergedEvents_schema.json")
  private String _schemaFile;

  @Option(name = "-help", help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;

  public void setPersonalAccessToken(String personalAccessToken) {
    _personalAccessToken = personalAccessToken;
  }

  public void setKafkaBrokerList(String kafkaBrokerList) {
    _kafkaBrokerList = kafkaBrokerList;
  }

  public void setTopic(String topic) {
    _topic = topic;
  }

  public void setEventType(String eventType) {
    _eventType = eventType;
  }

  public void setSchemaFile(String schemaFile) {
    _schemaFile = schemaFile;
  }

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "StreamGitHubEvents";
  }

  @Override
  public String toString() {
    return ("StreamGitHubEvents -personalAccessToken " + _personalAccessToken + " -kafkaBrokerList " + _kafkaBrokerList
        + " -topic " + _topic + " eventType " + _eventType + " schemaFile " + _schemaFile);
  }

  @Override
  public void cleanup() {
  }

  @Override
  public String description() {
    return "Streams GitHubEvents into a Kafka topic";
  }

  @Override
  public boolean execute()
      throws Exception {
    PluginManager.get().init();
    if (PULL_REQUEST_MERGED_EVENT_TYPE.equals(_eventType)) {
      PullRequestMergedEventsStream
          pullRequestMergedEventsStream = new PullRequestMergedEventsStream(_schemaFile, _topic, _kafkaBrokerList, _personalAccessToken);
      pullRequestMergedEventsStream.execute();
    } else {
      throw new UnsupportedOperationException("Event type " + _eventType + " is unsupported");
    }
    return true;
  }
}
