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
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.QuickStartBase;
import org.apache.pinot.tools.Quickstart;
import org.apache.pinot.tools.streams.PinotSourceDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The class that pulls events from GitHub by RPC calls, and converts them into byte[] so we can write to Kafka
 */
public class GithubPullRequestSourceGenerator implements PinotSourceDataGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(GithubPullRequestSourceGenerator.class);
  private static final long SLEEP_MILLIS = 10_000;

  private GitHubAPICaller _gitHubAPICaller;
  private Schema _avroSchema;
  private String _etag = null;

  public GithubPullRequestSourceGenerator(File schemaFile, String personalAccessToken)
      throws Exception {
    try {
      _avroSchema = AvroUtils.getAvroSchemaFromPinotSchema(org.apache.pinot.spi.data.Schema.fromFile(schemaFile));
    } catch (Exception e) {
      LOGGER.error("Got exception while reading Pinot schema from file: [" + schemaFile.getName() + "]");
      throw e;
    }
    _gitHubAPICaller = new GitHubAPICaller(personalAccessToken);
  }

  private GenericRecord convertToPullRequestMergedGenericRecord(JsonNode event)
      throws IOException {
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

          if (commitsResponse._responseString != null) {
            commits = JsonUtils.stringToJsonNode(commitsResponse._responseString);
          }

          JsonNode reviewComments = null;
          String reviewCommentsURL = pullRequest.get("review_comments_url").asText();
          GitHubAPICaller.GitHubAPIResponse reviewCommentsResponse = _gitHubAPICaller.callAPI(reviewCommentsURL);
          if (reviewCommentsResponse._responseString != null) {
            reviewComments = JsonUtils.stringToJsonNode(reviewCommentsResponse._responseString);
          }

          JsonNode comments = null;
          String commentsURL = pullRequest.get("comments_url").asText();
          GitHubAPICaller.GitHubAPIResponse commentsResponse = _gitHubAPICaller.callAPI(commentsURL);
          if (commentsResponse._responseString != null) {
            comments = JsonUtils.stringToJsonNode(commentsResponse._responseString);
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

  @Override
  public void init(Properties properties) {
  }

  @Override
  public List<StreamDataProducer.RowWithKey> generateRows() {
    List<StreamDataProducer.RowWithKey> retVal = new ArrayList<>();
    try {
      GitHubAPICaller.GitHubAPIResponse githubAPIResponse = _gitHubAPICaller.callEventsAPI(_etag);
      switch (githubAPIResponse._statusCode) {
        case 200: // Read new events
          _etag = githubAPIResponse._etag;
          JsonNode jsonArray = JsonUtils.stringToJsonNode(githubAPIResponse._responseString);
          for (JsonNode eventElement : jsonArray) {
            try {
              GenericRecord genericRecord = convertToPullRequestMergedGenericRecord(eventElement);
              if (genericRecord != null) {
                QuickStartBase.printStatus(Quickstart.Color.CYAN, genericRecord.toString());
                retVal.add(
                    new StreamDataProducer.RowWithKey(null, genericRecord.toString().getBytes(StandardCharsets.UTF_8)));
              }
            } catch (Exception e) {
              LOGGER.error("Exception in publishing generic record. Skipping", e);
            }
          }
          break;
        case 304: // Not Modified
          Quickstart.printStatus(Quickstart.Color.YELLOW, "Not modified. Checking again in 10s.");
          Thread.sleep(SLEEP_MILLIS);
          break;
        case 408: // Timeout
          Quickstart.printStatus(Quickstart.Color.YELLOW, "Timeout. Trying again in 10s.");
          Thread.sleep(SLEEP_MILLIS);
          break;
        case 403: // Rate Limit exceeded
          Quickstart.printStatus(Quickstart.Color.YELLOW,
              "Rate limit exceeded, sleeping until " + githubAPIResponse._resetTimeMs);
          long sleepMs = Math.max(60_000L, githubAPIResponse._resetTimeMs - System.currentTimeMillis());
          Thread.sleep(sleepMs);
          break;
        case 401: // Unauthorized
          String msg = "Unauthorized call to GitHub events API. Status message: " + githubAPIResponse._statusMessage
              + ". Exiting.";
          Quickstart.printStatus(Quickstart.Color.YELLOW, msg);
          throw new RuntimeException(msg);
        default: // Unknown status code
          Quickstart.printStatus(Quickstart.Color.YELLOW,
              "Unknown status code " + githubAPIResponse._statusCode + " statusMessage "
                  + githubAPIResponse._statusMessage + ". Retry in 10s");
          Thread.sleep(SLEEP_MILLIS);
          break;
      }
    } catch (Exception e) {
      LOGGER.error("Exception in reading events data", e);
      try {
        Thread.sleep(SLEEP_MILLIS);
      } catch (InterruptedException ex) {
        LOGGER.error("Caught exception in retry", ex);
      }
    }
    return retVal;
  }

  @Override
  public void close()
      throws Exception {
    _gitHubAPICaller.shutdown();
  }
}
