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

import com.google.common.base.Preconditions;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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

  private ExecutorService _service;
  private boolean _keepStreaming = true;

  private Schema _avroSchema;
  private String _topicName;
  private GithubAPICaller _githubAPICaller;

  private StreamDataProducer _producer;

  public PullRequestMergedEventsStream(String schemaFilePath, String topicName, String kafkaBrokerList,
      String personalAccessToken)
      throws Exception {

    _service = Executors.newFixedThreadPool(1);
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
    _githubAPICaller = new GithubAPICaller(personalAccessToken);

    Properties properties = new Properties();
    properties.put("metadata.broker.list", kafkaBrokerList);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");
    _producer = StreamDataProvider.getStreamDataProducer(KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME, properties);
  }

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

  public void shutdown()
      throws IOException, InterruptedException {
    printStatus(Quickstart.Color.GREEN, "***** Shutting down pullRequestMergedEvents Stream *****");
    _keepStreaming = false;
    Thread.sleep(3000L);
    _githubAPICaller.shutdown();
    _producer.close();
    _producer = null;
    _service.shutdown();
  }

  /**
   * Publishes the message to the kafka topic
   */
  private void publish(GenericRecord message)
      throws IOException {
    if (!_keepStreaming) {
      return;
    }
    printStatus(Quickstart.Color.YELLOW,  "Publishing");
    _producer.produce(_topicName, message.toString().getBytes(StandardCharsets.UTF_8));
    printStatus(Quickstart.Color.YELLOW,  "Published");
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
          printStatus(Quickstart.Color.YELLOW,  "Calling events api..");
          GithubAPICaller.GithubAPIResponse githubAPIResponse = _githubAPICaller.callEventsAPI(etag);
          printStatus(Quickstart.Color.YELLOW,  "Called api..");
          int statusCode = githubAPIResponse.statusCode;
          switch (statusCode) {
            case 200:
              etag = githubAPIResponse.etag;
              String responseString = githubAPIResponse.responseString;
              printStatus(Quickstart.Color.YELLOW,  "parsing " + statusCode);
              JsonArray jsonArray = new JsonParser().parse(responseString).getAsJsonArray();
              printStatus(Quickstart.Color.YELLOW,  "parsed " + statusCode);

              for (JsonElement eventElement : jsonArray) {
                try {
                  GenericRecord genericRecord = convertToPullRequestMergedGenericRecord(eventElement);
                  if (genericRecord != null) {
                    printStatus(Quickstart.Color.CYAN,  genericRecord.toString());
                    publish(genericRecord);
                  }
                } catch (Exception e) {
                  printStatus(Quickstart.Color.YELLOW,  "Exception in publishing generic record. Skipping." + e.getMessage());
                  LOGGER.error("Exception in publishing generic record. Skipping", e);
                }
              }
              break;
            case 304:
              // Not Modified - check again in 10 seconds
              printStatus(Quickstart.Color.YELLOW,  "Not modified. Check again in 10s.");
              Thread.sleep(10000L);
              break;
            case 403:
              // Rate Limit exceeded
              if (githubAPIResponse.remainingLimit == 0) {
                LOGGER.warn("Rate limit exceeded, retry after 1 minute");
                printStatus(Quickstart.Color.YELLOW,  "Rate limit exceeded, retry after 1 minute");
                // TODO: get renewal time from header. Github won't allow retry until 60 minutes
                Thread.sleep(60000L);
                break;
              }
            default:
              printStatus(Quickstart.Color.YELLOW,  "Status code " + statusCode + " statusMessage " + githubAPIResponse.statusMessage);
              throw new IllegalStateException(
                  "Received statusCode: " + statusCode + ", statusMessage: " + githubAPIResponse.statusMessage
                      + ", from events API. Exiting.");
          }
        } catch (Exception e) {
          printStatus(Quickstart.Color.YELLOW,  "Exception in reading events data");
          LOGGER.error("Exception in reading events data", e);
          return;
        }
        printStatus(Quickstart.Color.YELLOW,  "Continuing.." + _keepStreaming);
      }
    });
  }

  /**
   * Checks for events of type PullRequestEvent which have action = closed and merged = true.
   * Find commits, review comments, comments corresponding to this pull request event.
   * Construct a PullRequestMergedEvent with the help of the event, commits, review comments and comments.
   * Converts PullRequestMergedEvent to GenericRecord
   */
  private GenericRecord convertToPullRequestMergedGenericRecord(JsonElement eventJson)
      throws IOException {
    GenericRecord genericRecord = null;
    JsonObject event = eventJson.getAsJsonObject();
    String type = event.get("type").getAsString();

    printStatus(Quickstart.Color.YELLOW,  "Convert to pr event");

    try {
      if ("PullRequestEvent".equals(type)) {
        printStatus(Quickstart.Color.YELLOW, "PR event");
        JsonObject payload = event.get("payload").getAsJsonObject();
        if (payload != null) {
          String action = payload.get("action").getAsString();
          JsonObject pullRequest = payload.get("pull_request").getAsJsonObject();
          String merged = pullRequest.get("merged").getAsString();
          if ("closed".equals(action) && "true".equals(merged)) { // valid pull request merge event

            printStatus(Quickstart.Color.YELLOW,  "Closed and merged");
            JsonArray commits = null;
            try {
              // get commits
              printStatus(Quickstart.Color.YELLOW,  "Get commits");
              String commitsURL = pullRequest.get("commits_url").getAsString();
              GithubAPICaller.GithubAPIResponse commitsResponse = _githubAPICaller.executeGet(commitsURL);

              if (commitsResponse.responseString != null) {
                commits = new JsonParser().parse(commitsResponse.responseString).getAsJsonArray();
              }
              printStatus(Quickstart.Color.YELLOW,  "Got commits");
            } catch (Exception e) {
              printStatus(Quickstart.Color.YELLOW,  "Exception in commits" + e.getMessage());
              throw e;
            }

            JsonArray reviewComments = null;
            try {
              // get review comments
              printStatus(Quickstart.Color.YELLOW,  "Get review comments");
              String reviewCommentsURL = pullRequest.get("review_comments_url").getAsString();
              GithubAPICaller.GithubAPIResponse reviewCommentsResponse = _githubAPICaller.executeGet(reviewCommentsURL);

              if (reviewCommentsResponse.responseString != null) {
                reviewComments = new JsonParser().parse(reviewCommentsResponse.responseString).getAsJsonArray();
              }
              printStatus(Quickstart.Color.YELLOW,  "Got review comments");
            } catch (Exception e) {
              printStatus(Quickstart.Color.YELLOW,  "Exception in review comments" + e.getMessage());
              throw e;
            }

            JsonArray comments = null;
            try {
              // get comments
              printStatus(Quickstart.Color.YELLOW,  "Get comments");
              String commentsURL = pullRequest.get("comments_url").getAsString();
              GithubAPICaller.GithubAPIResponse commentsResponse = _githubAPICaller.executeGet(commentsURL);

              if (commentsResponse.responseString != null) {
                comments = new JsonParser().parse(commentsResponse.responseString).getAsJsonArray();
              }
              printStatus(Quickstart.Color.YELLOW,  "Got comments");
            } catch (Exception e) {
              printStatus(Quickstart.Color.YELLOW,  "Exception in comments" + e.getMessage());
              throw e;
            }

            // get PullRequestMergeEvent
            PullRequestMergedEvent pullRequestMergedEvent = new PullRequestMergedEvent(event, commits, reviewComments, comments);
            printStatus(Quickstart.Color.YELLOW,  "Made pr event");
            // make generic record
            genericRecord = convertToGenericRecord(pullRequestMergedEvent);
            printStatus(Quickstart.Color.YELLOW,  "made generic record");
          }
        }
      }
    } catch (Exception e) {
      printStatus(Quickstart.Color.YELLOW,  "Exception " + e.getMessage());
    }
    printStatus(Quickstart.Color.YELLOW,  "Return from conversion");
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

  public static void main(String[] args)
      throws Exception {
    String personalAccessToken = args[0];
    String schemaFile = args[1];
    String topic = "pullRequestMergedEvent";
    PullRequestMergedEventsStream stream =
        new PullRequestMergedEventsStream(schemaFile, topic, KafkaStarterUtils.DEFAULT_KAFKA_BROKER,
            personalAccessToken);
    stream.execute();
  }
}
