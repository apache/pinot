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
package org.apache.pinot.tools.streams.githubevents;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.Quickstart;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.tools.Quickstart.printStatus;


/**
 * Creates a Kafka producer, for given kafka broker list
 * Continuously fetches github events data.
 * Creates a PullRequestMergedEvent for each valid PR event.
 * Publishes the PullRequestMergedEvent to the given kafka topic
 */
public class PullRequestMergedEventsStream {
  private static final Logger LOGGER = LoggerFactory.getLogger(PullRequestMergedEventsStream.class);
  private static final long SLEEP_MILLIS = 10_000;

  private final ExecutorService _service;
  private boolean _keepStreaming = true;

  private final Schema _avroSchema;
  private final String _topicName;
  private final GitHubAPICaller _gitHubAPICaller;

  private StreamDataProducer _producer;

  public PullRequestMergedEventsStream(String schemaFilePath, String topicName, String kafkaBrokerList,
      String personalAccessToken) throws Exception {

    _service = Executors.newFixedThreadPool(2);
    try {
      File pinotSchema;
      if (schemaFilePath == null) {
        ClassLoader classLoader = PullRequestMergedEventsStream.class.getClassLoader();
        URL resource = classLoader.getResource("examples/stream/githubEvents/pullRequestMergedEvents_schema.json");
        Preconditions.checkNotNull(resource);
        pinotSchema = new File(resource.getFile());
      } else {
        pinotSchema = new File(schemaFilePath);
      }
      _avroSchema = AvroUtils.getAvroSchemaFromPinotSchema(org.apache.pinot.spi.data.Schema.fromFile(pinotSchema));
    } catch (Exception e) {
      LOGGER.error("Got exception while reading Pinot schema from file: [" + schemaFilePath + "]");
      throw e;
    }
    _topicName = topicName;
    _gitHubAPICaller = new GitHubAPICaller(personalAccessToken);

    Properties properties = new Properties();
    properties.put("metadata.broker.list", kafkaBrokerList);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");
    _producer = StreamDataProvider.getStreamDataProducer(KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME, properties);
  }

  public static void main(String[] args) throws Exception {
    String personalAccessToken = args[0];
    String schemaFile = args[1];
    String topic = "pullRequestMergedEvent";
    PullRequestMergedEventsStream stream = new PullRequestMergedEventsStream(schemaFile, topic,
        KafkaStarterUtils.DEFAULT_KAFKA_BROKER, personalAccessToken);
    stream.execute();
  }

  /**
   * Starts the stream.
   * Adds shutdown hook.
   */
  public void execute() {
    start();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));
  }

  /**
   * Shuts down the stream.
   */
  public void shutdown() throws IOException, InterruptedException {
    printStatus(Quickstart.Color.GREEN, "***** Shutting down pullRequestMergedEvents Stream *****");
    _keepStreaming = false;
    Thread.sleep(3000L);
    _gitHubAPICaller.shutdown();
    _producer.close();
    _producer = null;
    _service.shutdown();
  }

  /**
   * Publishes the message to the kafka topic
   */
  private void publish(GenericRecord message) throws IOException {
    if (!_keepStreaming) {
      return;
    }
    _producer.produce(_topicName, message.toString().getBytes(StandardCharsets.UTF_8));
  }

  public void start() {

    printStatus(Quickstart.Color.CYAN, "***** Starting pullRequestMergedEvents Stream *****");

    _service.submit(() -> {

      String etag = null;
      while (true) {
        if (!_keepStreaming) {
          return;
        }
        try {
          GitHubAPICaller.GitHubAPIResponse githubAPIResponse = _gitHubAPICaller.callEventsAPI(etag);
          switch (githubAPIResponse.statusCode) {
            case 200: // Read new events
              etag = githubAPIResponse.etag;
              JsonNode jsonArray = JsonUtils.stringToJsonNode(githubAPIResponse.responseString);
              for (JsonNode eventElement : jsonArray) {
                try {
                  GenericRecord genericRecord = convertToPullRequestMergedGenericRecord(eventElement);
                  if (genericRecord != null) {
                    printStatus(Quickstart.Color.CYAN, genericRecord.toString());
                    publish(genericRecord);
                  }
                } catch (Exception e) {
                  LOGGER.error("Exception in publishing generic record. Skipping", e);
                }
              }
              break;
            case 304: // Not Modified
              printStatus(Quickstart.Color.YELLOW, "Not modified. Checking again in 10s.");
              Thread.sleep(SLEEP_MILLIS);
              break;
            case 408: // Timeout
              printStatus(Quickstart.Color.YELLOW, "Timeout. Trying again in 10s.");
              Thread.sleep(SLEEP_MILLIS);
              break;
            case 403: // Rate Limit exceeded
              printStatus(Quickstart.Color.YELLOW,
                  "Rate limit exceeded, sleeping until " + githubAPIResponse.resetTimeMs);
              long sleepMs = Math.max(60_000L, githubAPIResponse.resetTimeMs - System.currentTimeMillis());
              Thread.sleep(sleepMs);
              break;
            case 401: // Unauthorized
              printStatus(Quickstart.Color.YELLOW, "Unauthorized call to GitHub events API. Status message: "
                  + githubAPIResponse.statusMessage + ". Exiting.");
              return;
            default: // Unknown status code
              printStatus(Quickstart.Color.YELLOW, "Unknown status code " + githubAPIResponse.statusCode
                  + " statusMessage " + githubAPIResponse.statusMessage + ". Retry in 10s");
              Thread.sleep(SLEEP_MILLIS);
          }
        } catch (Exception e) {
          LOGGER.error("Exception in reading events data", e);
          try {
            Thread.sleep(SLEEP_MILLIS);
          } catch (InterruptedException ex) {
            LOGGER.error("Caught exception in retry", ex);
          }
        }
      }
    });
  }

  /**
   * Checks for events of type PullRequestEvent which have action = closed and merged = true.
   * Find commits, review comments, comments corresponding to this pull request event.
   * Construct a PullRequestMergedEvent with the help of the event, commits, review comments and comments.
   * Converts PullRequestMergedEvent to GenericRecord
   * @param event
   */
  private GenericRecord convertToPullRequestMergedGenericRecord(JsonNode event) throws IOException {
    GenericRecord genericRecord = null;
    String type = event.get("type").asText();

    if ("PullRequestEvent".equals(type)) {
      JsonNode payload = event.get("payload");
      if (payload != null) {
        String action = payload.get("action").asText();
        JsonNode pullRequest = payload.get("pull_request");
        String merged = pullRequest.get("merged").asText();
        if ("closed".equals(action) && "true".equals(merged)) { // valid pull request merge event

          JsonNode commits = null;
          String commitsURL = pullRequest.get("commits_url").asText();
          GitHubAPICaller.GitHubAPIResponse commitsResponse = _gitHubAPICaller.callAPI(commitsURL);

          if (commitsResponse.responseString != null) {
            commits = JsonUtils.stringToJsonNode(commitsResponse.responseString);
          }

          JsonNode reviewComments = null;
          String reviewCommentsURL = pullRequest.get("review_comments_url").asText();
          GitHubAPICaller.GitHubAPIResponse reviewCommentsResponse = _gitHubAPICaller.callAPI(reviewCommentsURL);
          if (reviewCommentsResponse.responseString != null) {
            reviewComments = JsonUtils.stringToJsonNode(reviewCommentsResponse.responseString);
          }

          JsonNode comments = null;
          String commentsURL = pullRequest.get("comments_url").asText();
          GitHubAPICaller.GitHubAPIResponse commentsResponse = _gitHubAPICaller.callAPI(commentsURL);
          if (commentsResponse.responseString != null) {
            comments = JsonUtils.stringToJsonNode(commentsResponse.responseString);
          }

          // get PullRequestMergeEvent
          PullRequestMergedEvent pullRequestMergedEvent =
              new PullRequestMergedEvent(event, commits, reviewComments, comments);
          // make generic record
          genericRecord = convertToGenericRecord(pullRequestMergedEvent);
        }
      }
    }
    return genericRecord;
  }

  /**
   * Convert the PullRequestMergedEvent to a GenericRecord
   */
  private GenericRecord convertToGenericRecord(PullRequestMergedEvent pullRequestMergedEvent) {
    GenericRecord genericRecord = new GenericData.Record(_avroSchema);

    // Dimensions
    genericRecord.put("title", pullRequestMergedEvent.getTitle());
    genericRecord.put("labels", pullRequestMergedEvent.getLabels());
    genericRecord.put("userId", pullRequestMergedEvent.getUserId());
    genericRecord.put("userType", pullRequestMergedEvent.getUserType());
    genericRecord.put("authorAssociation", pullRequestMergedEvent.getAuthorAssociation());
    genericRecord.put("mergedBy", pullRequestMergedEvent.getMergedBy());
    genericRecord.put("assignees", pullRequestMergedEvent.getAssignees());
    genericRecord.put("committers", pullRequestMergedEvent.getCommitters());
    genericRecord.put("reviewers", pullRequestMergedEvent.getReviewers());
    genericRecord.put("commenters", pullRequestMergedEvent.getCommenters());
    genericRecord.put("authors", pullRequestMergedEvent.getAuthors());
    genericRecord.put("requestedReviewers", pullRequestMergedEvent.getRequestedReviewers());
    genericRecord.put("requestedTeams", pullRequestMergedEvent.getRequestedTeams());
    genericRecord.put("repo", pullRequestMergedEvent.getRepo());
    genericRecord.put("organization", pullRequestMergedEvent.getOrganization());

    // Metrics
    genericRecord.put("numComments", pullRequestMergedEvent.getNumComments());
    genericRecord.put("numReviewComments", pullRequestMergedEvent.getNumReviewComments());
    genericRecord.put("numCommits", pullRequestMergedEvent.getNumCommits());
    genericRecord.put("numLinesAdded", pullRequestMergedEvent.getNumLinesAdded());
    genericRecord.put("numLinesDeleted", pullRequestMergedEvent.getNumLinesDeleted());
    genericRecord.put("numFilesChanged", pullRequestMergedEvent.getNumFilesChanged());
    genericRecord.put("numReviewers", pullRequestMergedEvent.getNumReviewers());
    genericRecord.put("numCommenters", pullRequestMergedEvent.getNumCommenters());
    genericRecord.put("numCommitters", pullRequestMergedEvent.getNumCommitters());
    genericRecord.put("numAuthors", pullRequestMergedEvent.getNumAuthors());
    genericRecord.put("createdTimeMillis", pullRequestMergedEvent.getCreatedTimeMillis());
    genericRecord.put("elapsedTimeMillis", pullRequestMergedEvent.getElapsedTimeMillis());

    // Time column
    genericRecord.put("mergedTimeMillis", pullRequestMergedEvent.getMergedTimeMillis());

    return genericRecord;
  }
}
