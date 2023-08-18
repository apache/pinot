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
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.streams.githubevents.PullRequestMergedEventsStream;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.apache.pinot.tools.utils.StreamSourceType;
import picocli.CommandLine;


/**
 * Command to stream GitHub events into a kafka topic or kinesis stream
 */
@CommandLine.Command(name = "StreamGitHubEvents", description = "Streams GitHubEvents into a Kafka topic or Kinesis "
                                                                + "Stream", mixinStandardHelpOptions = true)
public class StreamGitHubEventsCommand extends AbstractBaseAdminCommand implements Command {

  private static final String PULL_REQUEST_MERGED_EVENT_TYPE = "pullRequestMergedEvent";

  @CommandLine.Option(names = {"-personalAccessToken"}, required = true, description = "GitHub personal access token.")
  private String _personalAccessToken;

  @CommandLine.Option(names = {"-sourceType"}, defaultValue = "Kafka", description = "Stream DataSource to use for "
      + "ingesting data. Supported values - Kafka,Kinesis")
  private String _sourceType;

  @CommandLine.Option(names = {"-kafkaBrokerList"}, description = "Kafka broker list of the kafka cluster to produce "
      + "events.")
  private String _kafkaBrokerList = KafkaStarterUtils.DEFAULT_KAFKA_BROKER;

  @CommandLine.Option(names = {"-kafkaSecurityProtocol"}, description = "Kafka security protocol "
      + "(PLAINTEXT/SASL_SSL/etc")
  private String _kafkaSecurityProtocol;

  // Only needed when configured as SASL_SSL
  @CommandLine.Option(names = {"-kafkaUserName"}, description = "Kafka Username")
  private String _kafkaSaslUserName;

  @CommandLine.Option(names = {"-kafkaPassword"}, description = "Kafka Password")
  private String _kafkaSaslPassword;

  @CommandLine.Option(names = {"-kinesisEndpoint"}, description = "Endpoint of localstack or any other Kinesis "
      + "cluster when not using AWS.")
  private String _kinesisEndpoint = null;

  @CommandLine.Option(names = {"-awsRegion"}, description = "AWS Region in which Kinesis is located")
  private String _awsRegion = "us-east-1";

  @CommandLine.Option(names = {"-awsAccessKey"}, description = "AccessKey for AWS Account")
  private String _accessKey;

  @CommandLine.Option(names = {"-awsSecretKey"}, description = "SecretKey for AWS Account")
  private String _secretKey;

  // Pulsar related Configs
  @CommandLine.Option(names = {"-pulsarBrokerServiceUrl"}, description = "Pulsar broker service url.")
  private String _pulsarBrokerServiceUrl;

  @CommandLine.Option(names = {"-token"}, description = "Pulsar token.")
  private String _pulsarToken;

  @CommandLine.Option(names = {"-topic"}, required = true, description = "Name of kafka-topic/kinesis-stream to "
      + "publish events.")
  private String _topic;

  @CommandLine.Option(names = {"-eventType"}, description = "Type of GitHub event. Supported types - "
      + "pullRequestMergedEvent")
  private String _eventType = PULL_REQUEST_MERGED_EVENT_TYPE;

  @CommandLine.Option(names = {"-schemaFile"}, description = "Path to schema file. "
      + "By default uses examples/stream/pullRequestMergedEvents/pullRequestMergedEvents_schema.json")
  private String _schemaFile;

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
  public boolean execute()
      throws Exception {
    PluginManager.get().init();
    if (PULL_REQUEST_MERGED_EVENT_TYPE.equals(_eventType)) {
      StreamDataProducer streamDataProducer;
      switch (StreamSourceType.valueOf(_sourceType.toUpperCase())) {
        case KINESIS:
          streamDataProducer =
              PullRequestMergedEventsStream.getKinesisStreamDataProducer(_kinesisEndpoint, _awsRegion, _accessKey,
                  _secretKey);
          break;
        case PULSAR:
          streamDataProducer =
              PullRequestMergedEventsStream.getPulsarStreamDataProducer(_pulsarBrokerServiceUrl, _pulsarToken);
          break;
        case KAFKA:
        default:
          streamDataProducer =
              PullRequestMergedEventsStream.getKafkaStreamDataProducer(_kafkaBrokerList, _kafkaSecurityProtocol,
                  _kafkaSaslUserName, _kafkaSaslPassword);
          break;
      }
      PullRequestMergedEventsStream pullRequestMergedEventsStream =
          new PullRequestMergedEventsStream(_schemaFile, _topic, _personalAccessToken, streamDataProducer);
      pullRequestMergedEventsStream.execute();
    } else {
      throw new UnsupportedOperationException("Event type " + _eventType + " is unsupported");
    }
    return true;
  }
}
