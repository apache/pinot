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

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
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
  private String _title;
  private List<String> _labels;
  private String _userId;
  private String _userType;
  private String _authorAssociation;
  private String _mergedBy;
  private List<String> _assignees;
  private List<String> _committers;
  private List<String> _authors;
  private List<String> _reviewers;
  private List<String> _commenters;
  private List<String> _requestedReviewers;
  private List<String> _requestedTeams;
  private String _repo;
  private String _organization;

  // metrics
  private long _numComments;
  private long _numReviewComments;
  private long _numCommits;
  private long _numLinesAdded;
  private long _numLinesDeleted;
  private long _numFilesChanged;
  private long _numReviewers;
  private long _numCommenters;
  private long _numCommitters;
  private long _numAuthors;
  private long _createdTimeMillis;
  private long _elapsedTimeMillis;

  // time
  private long _mergedTimeMillis;

  private JsonObject _pullRequest;
  private JsonArray _commitsArray;
  private JsonArray _reviewCommentsArray;
  private JsonArray _commentsArray;

  /**
   * Construct a PullRequestMergedEvent from the github event of type PullRequestEvent which is also merged and closed.
   * Note that this constructor assumes that the event is valid of this type
   * @param event - event of type PullRequestEvent which has action closed and is merged
   * @param commits - commits data corresponding to the event
   * @param reviewComments - review comments data corresponding to the event
   * @param comments - comments data corresponding to the event
   */
  public PullRequestMergedEvent(JsonObject event, JsonArray commits, JsonArray reviewComments, JsonArray comments) {

    JsonObject payload = event.get("payload").getAsJsonObject();
    _pullRequest = payload.get("pull_request").getAsJsonObject();
    _commitsArray = commits;
    _reviewCommentsArray = reviewComments;
    _commentsArray = comments;

    // Dimensions
    _title = _pullRequest.get("title").getAsString();
    _labels = extractLabels();
    JsonObject user = _pullRequest.get("user").getAsJsonObject();
    _userId = user.get("login").getAsString();
    _userType = user.get("type").getAsString();
    _authorAssociation = _pullRequest.get("author_association").getAsString();
    JsonObject mergedBy = _pullRequest.get("merged_by").getAsJsonObject();
    _mergedBy = mergedBy.get("login").getAsString();
    _assignees = extractAssignees();
    _committers = Lists.newArrayList(extractCommitters());
    _authors = Lists.newArrayList(extractAuthors());
    _reviewers = Lists.newArrayList(extractReviewers());
    _commenters = Lists.newArrayList(extractCommenters());
    _requestedReviewers = extractRequestedReviewers();
    _requestedTeams = extractRequestedTeams();
    JsonObject repo = event.get("repo").getAsJsonObject();
    String[] repoName = repo.get("name").getAsString().split("/");
    _repo = repoName[1];
    _organization = repoName[0];

    // Metrics
    _numComments = _pullRequest.get("comments").getAsInt();
    _numReviewComments = _pullRequest.get("review_comments").getAsInt();
    _numCommits = _pullRequest.get("commits").getAsInt();
    _numLinesAdded = _pullRequest.get("additions").getAsInt();
    _numLinesDeleted = _pullRequest.get("deletions").getAsInt();
    _numFilesChanged = _pullRequest.get("changed_files").getAsInt();
    _numReviewers = _reviewers.size();
    _numCommenters = _commenters.size();
    _numCommitters = _committers.size();
    _numAuthors = _authors.size();

    // Time
    _createdTimeMillis = DATE_FORMATTER.parseMillis(_pullRequest.get("created_at").getAsString());
    _mergedTimeMillis = DATE_FORMATTER.parseMillis(_pullRequest.get("merged_at").getAsString());
    _elapsedTimeMillis = _mergedTimeMillis - _createdTimeMillis;
  }

  /**
   * Extracts reviewer userIds for the PR
   */
  private Set<String> extractReviewers() {
    Set<String> reviewers;
    if (_reviewCommentsArray != null && !_reviewCommentsArray.isJsonNull() & _reviewCommentsArray.size() > 0) {
      reviewers = new HashSet<>();
      for (JsonElement reviewComment : _reviewCommentsArray) {
        reviewers.add(reviewComment.getAsJsonObject().get("user").getAsJsonObject().get("login").getAsString());
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
    if (_commentsArray != null && !_commentsArray.isJsonNull() & _commentsArray.size() > 0) {
      commenters = new HashSet<>();
      for (JsonElement comment : _commentsArray) {
        commenters.add(comment.getAsJsonObject().get("user").getAsJsonObject().get("login").getAsString());
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
    if (_commitsArray != null && !_commitsArray.isJsonNull() & _commitsArray.size() > 0) {
      committers = new HashSet<>();
      for (JsonElement commit : _commitsArray) {
        JsonObject commitAsJsonObject = commit.getAsJsonObject();
        JsonElement committer = commitAsJsonObject.get("committer");
        if (committer.isJsonNull()) {
          committers.add(
              commitAsJsonObject.get("commit").getAsJsonObject().get("committer").getAsJsonObject().get("name")
                  .getAsString());
        } else {
          committers.add(committer.getAsJsonObject().get("login").getAsString());
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
    if (_commitsArray != null && !_commitsArray.isJsonNull() & _commitsArray.size() > 0) {
      authors = new HashSet<>();
      for (JsonElement commit : _commitsArray) {
        JsonObject commitAsJsonObject = commit.getAsJsonObject();
        JsonElement author = commitAsJsonObject.get("author");
        if (author.isJsonNull()) {
          authors.add(commitAsJsonObject.get("commit").getAsJsonObject().get("author").getAsJsonObject().get("name")
              .getAsString());
        } else {
          authors.add(author.getAsJsonObject().get("login").getAsString());
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
    JsonArray labelsArray = _pullRequest.get("labels").getAsJsonArray();
    List<String> labels;
    int size = labelsArray.size();
    if (size > 0) {
      labels = new ArrayList<>(size);
      for (JsonElement label : labelsArray) {
        labels.add(label.getAsJsonObject().get("name").getAsString());
      }
    } else {
      labels = Collections.emptyList();
    }
    return labels;
  }

  /**
   * Extracts assignees for the PR
   */
  private List<String> extractAssignees() {
    JsonArray assigneesJson = _pullRequest.get("assignees").getAsJsonArray();
    int numAssignees = assigneesJson.size();
    List<String> assignees;
    if (numAssignees > 0) {
      assignees = new ArrayList<>(numAssignees);
      for (JsonElement reviewer : assigneesJson) {
        assignees.add(reviewer.getAsJsonObject().get("login").getAsString());
      }
    } else {
      assignees = Collections.emptyList();
    }
    return assignees;
  }

  /**
   * Extracts list of requested reviewers
   */
  private List<String> extractRequestedReviewers() {
    JsonArray requestedReviewersJson = _pullRequest.get("requested_reviewers").getAsJsonArray();
    int numRequestedReviewers = requestedReviewersJson.size();
    List<String> requestedReviewers;
    if (numRequestedReviewers > 0) {
      requestedReviewers = new ArrayList<>(numRequestedReviewers);
      for (JsonElement reviewer : requestedReviewersJson) {
        requestedReviewers.add(reviewer.getAsJsonObject().get("login").getAsString());
      }
    } else {
      requestedReviewers = Collections.emptyList();
    }
    return requestedReviewers;
  }

  /**
   * Extracts list of review requested teams
   */
  private List<String> extractRequestedTeams() {
    JsonArray requestedTeamsJson = _pullRequest.get("requested_teams").getAsJsonArray();
    int numRequestedTeams = requestedTeamsJson.size();
    List<String> requestedTeams;
    if (numRequestedTeams > 0) {
      requestedTeams = new ArrayList<>(numRequestedTeams);
      for (JsonElement team : requestedTeamsJson) {
        requestedTeams.add(team.getAsJsonObject().get("name").getAsString());
      }
    } else {
      requestedTeams = Collections.emptyList();
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
