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
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;


/**
 * Represents the pull request merged event
 */
public class PullRequestMergedEvent {

  private static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.dateTimeNoMillis();

  // dimensions
  private final String _title;
  private final List<String> _labels;
  private final String _userId;
  private final String _userType;
  private final String _authorAssociation;
  private final String _mergedBy;
  private final List<String> _assignees;
  private final List<String> _committers;
  private final List<String> _authors;
  private final List<String> _reviewers;
  private final List<String> _commenters;
  private final List<String> _requestedReviewers;
  private final List<String> _requestedTeams;
  private final String _repo;
  private final String _organization;

  // metrics
  private final long _numComments;
  private final long _numReviewComments;
  private final long _numCommits;
  private final long _numLinesAdded;
  private final long _numLinesDeleted;
  private final long _numFilesChanged;
  private final long _numReviewers;
  private final long _numCommenters;
  private final long _numCommitters;
  private final long _numAuthors;
  private final long _createdTimeMillis;
  private final long _elapsedTimeMillis;

  // time
  private final long _mergedTimeMillis;

  private final JsonNode _pullRequest;
  private final JsonNode _commitsArray;
  private final JsonNode _reviewCommentsArray;
  private final JsonNode _commentsArray;

  /**
   * Construct a PullRequestMergedEvent from the github event of type PullRequestEvent which is also merged and closed.
   * Note that this constructor assumes that the event is valid of this type
   * @param event - event of type PullRequestEvent which has action closed and is merged
   * @param commits - commits data corresponding to the event
   * @param reviewComments - review comments data corresponding to the event
   * @param comments - comments data corresponding to the event
   */
  public PullRequestMergedEvent(JsonNode event, JsonNode commits, JsonNode reviewComments, JsonNode comments) {

    JsonNode payload = event.get("payload");
    _pullRequest = payload.get("pull_request");
    _commitsArray = commits;
    _reviewCommentsArray = reviewComments;
    _commentsArray = comments;

    // Dimensions
    _title = _pullRequest.get("title").asText();
    _labels = extractLabels();
    JsonNode user = _pullRequest.get("user");
    _userId = user.get("login").asText();
    _userType = user.get("type").asText();
    _authorAssociation = _pullRequest.get("author_association").asText();
    JsonNode mergedBy = _pullRequest.get("merged_by");
    _mergedBy = mergedBy.get("login").asText();
    _assignees = extractAssignees();
    _committers = Lists.newArrayList(extractCommitters());
    _authors = Lists.newArrayList(extractAuthors());
    _reviewers = Lists.newArrayList(extractReviewers());
    _commenters = Lists.newArrayList(extractCommenters());
    _requestedReviewers = extractRequestedReviewers();
    _requestedTeams = extractRequestedTeams();
    JsonNode repo = event.get("repo");
    String[] repoName = repo.get("name").asText().split("/");
    _repo = repoName[1];
    _organization = repoName[0];

    // Metrics
    _numComments = _pullRequest.get("comments").asInt();
    _numReviewComments = _pullRequest.get("review_comments").asInt();
    _numCommits = _pullRequest.get("commits").asInt();
    _numLinesAdded = _pullRequest.get("additions").asInt();
    _numLinesDeleted = _pullRequest.get("deletions").asInt();
    _numFilesChanged = _pullRequest.get("changed_files").asInt();
    _numReviewers = _reviewers.size();
    _numCommenters = _commenters.size();
    _numCommitters = _committers.size();
    _numAuthors = _authors.size();

    // Time
    _createdTimeMillis = DATE_FORMATTER.parseMillis(_pullRequest.get("created_at").asText());
    _mergedTimeMillis = DATE_FORMATTER.parseMillis(_pullRequest.get("merged_at").asText());
    _elapsedTimeMillis = _mergedTimeMillis - _createdTimeMillis;
  }

  /**
   * Extracts reviewer userIds for the PR
   */
  private Set<String> extractReviewers() {
    Set<String> reviewers;
    if (_reviewCommentsArray != null && !_reviewCommentsArray.isEmpty()) {
      reviewers = new HashSet<>();
      for (JsonNode reviewComment : _reviewCommentsArray) {
        reviewers.add(reviewComment.get("user").get("login").asText());
      }
    } else {
      reviewers = Collections.emptySet();
    }
    return reviewers;
  }

  /**
   * Extracts commenter names for the PR
   */
  private Set<String> extractCommenters() {
    Set<String> commenters;
    if (_commentsArray != null && !_commentsArray.isEmpty()) {
      commenters = new HashSet<>();
      for (JsonNode comment : _commentsArray) {
        commenters.add(comment.get("user").get("login").asText());
      }
    } else {
      commenters = Collections.emptySet();
    }
    return commenters;
  }

  /**
   * Extracts committer names for the PR
   */
  private Set<String> extractCommitters() {
    Set<String> committers;
    if (_commitsArray != null && !_commitsArray.isEmpty()) {
      committers = new HashSet<>();
      for (JsonNode commit : _commitsArray) {
        JsonNode commitAsJsonNode = commit;
        JsonNode committer = commitAsJsonNode.get("committer");
        if (committer.isEmpty()) {
          committers.add(commitAsJsonNode.get("commit").get("committer").get("name").asText());
        } else {
          committers.add(committer.get("login").asText());
        }
      }
    } else {
      committers = Collections.emptySet();
    }
    return committers;
  }

  /**
   * Extracts author names for the PR
   */
  private Set<String> extractAuthors() {
    Set<String> authors;
    if (_commitsArray != null && !_commitsArray.isEmpty()) {
      authors = new HashSet<>();
      for (JsonNode commit : _commitsArray) {
        JsonNode commitAsJsonNode = commit;
        JsonNode author = commitAsJsonNode.get("author");
        if (author.isEmpty()) {
          authors.add(commitAsJsonNode.get("commit").get("author").get("name").asText());
        } else {
          authors.add(author.get("login").asText());
        }
      }
    } else {
      authors = Collections.emptySet();
    }
    return authors;
  }

  /**
   * Extracts labels for the PR
   */
  private List<String> extractLabels() {
    Iterator<JsonNode> labelsIterator = _pullRequest.get("labels").elements();
    List<String> labels = new ArrayList<>();
    while (labelsIterator.hasNext()) {
      labels.add(labelsIterator.next().get("name").asText());
    }
    return labels;
  }

  /**
   * Extracts assignees for the PR
   */
  private List<String> extractAssignees() {
    Iterator<JsonNode> assigneesIterator = _pullRequest.get("assignees").elements();
    List<String> assignees = new ArrayList<>();
    while (assigneesIterator.hasNext()) {
      assignees.add(assigneesIterator.next().get("login").asText());
    }
    return assignees;
  }

  /**
   * Extracts list of requested reviewers
   */
  private List<String> extractRequestedReviewers() {
    Iterator<JsonNode> requestedReviewersIterator = _pullRequest.get("requested_reviewers").elements();
    List<String> requestedReviewers = new ArrayList<>();
    while (requestedReviewersIterator.hasNext()) {
      requestedReviewers.add(requestedReviewersIterator.next().get("login").asText());
    }
    return requestedReviewers;
  }

  /**
   * Extracts list of review requested teams
   */
  private List<String> extractRequestedTeams() {

    Iterator<JsonNode> requestedTeamsIterator = _pullRequest.get("requested_teams").elements();
    List<String> requestedTeams = new ArrayList<>();
    while (requestedTeamsIterator.hasNext()) {
      requestedTeams.add(requestedTeamsIterator.next().get("name").asText());
    }
    return requestedTeams;
  }

  public String getTitle() {
    return _title;
  }

  public List<String> getLabels() {
    return _labels;
  }

  public String getUserId() {
    return _userId;
  }

  public String getUserType() {
    return _userType;
  }

  public String getAuthorAssociation() {
    return _authorAssociation;
  }

  public String getMergedBy() {
    return _mergedBy;
  }

  public List<String> getReviewers() {
    return _reviewers;
  }

  public List<String> getAuthors() {
    return _authors;
  }

  public List<String> getCommenters() {
    return _commenters;
  }

  public long getNumAuthors() {
    return _numAuthors;
  }

  public String getRepo() {
    return _repo;
  }

  public String getOrganization() {
    return _organization;
  }

  public long getNumComments() {
    return _numComments;
  }

  public long getNumReviewComments() {
    return _numReviewComments;
  }

  public long getNumCommits() {
    return _numCommits;
  }

  public long getNumLinesAdded() {
    return _numLinesAdded;
  }

  public long getNumLinesDeleted() {
    return _numLinesDeleted;
  }

  public long getNumFilesChanged() {
    return _numFilesChanged;
  }

  public long getNumReviewers() {
    return _numReviewers;
  }

  public long getNumCommenters() {
    return _numCommenters;
  }

  public long getNumCommitters() {
    return _numCommitters;
  }

  public long getCreatedTimeMillis() {
    return _createdTimeMillis;
  }

  public long getElapsedTimeMillis() {
    return _elapsedTimeMillis;
  }

  public long getMergedTimeMillis() {
    return _mergedTimeMillis;
  }

  public List<String> getCommitters() {
    return _committers;
  }

  public List<String> getAssignees() {
    return _assignees;
  }

  public List<String> getRequestedReviewers() {
    return _requestedReviewers;
  }

  public List<String> getRequestedTeams() {
    return _requestedTeams;
  }
}
