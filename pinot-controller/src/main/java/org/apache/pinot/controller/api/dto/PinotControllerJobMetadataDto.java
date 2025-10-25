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
package org.apache.pinot.controller.api.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;


/**
 * Type-safe DTO (Data Transfer Object) for controller job ZK metadata.
 * Provides structured access to job metadata fields instead of using raw Map<String, String>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PinotControllerJobMetadataDto {
  private String _jobId;

  @JsonProperty("tableName")
  private String _tableNameWithType;

  private String _jobType;
  private long _submissionTimeMs;
  private int _messageCount;
  private String _segmentName;
  private String _instanceName;

  public String getJobId() {
    return _jobId;
  }

  public PinotControllerJobMetadataDto setJobId(String jobId) {
    _jobId = jobId;
    return this;
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public PinotControllerJobMetadataDto setTableNameWithType(String tableNameWithType) {
    _tableNameWithType = tableNameWithType;
    return this;
  }

  public String getJobType() {
    return _jobType;
  }

  public PinotControllerJobMetadataDto setJobType(String jobType) {
    _jobType = jobType;
    return this;
  }

  public long getSubmissionTimeMs() {
    return _submissionTimeMs;
  }

  public PinotControllerJobMetadataDto setSubmissionTimeMs(long submissionTimeMs) {
    _submissionTimeMs = submissionTimeMs;
    return this;
  }

  public int getMessageCount() {
    return _messageCount;
  }

  public PinotControllerJobMetadataDto setMessageCount(int messageCount) {
    _messageCount = messageCount;
    return this;
  }

  @Nullable
  public String getSegmentName() {
    return _segmentName;
  }

  public PinotControllerJobMetadataDto setSegmentName(@Nullable String segmentName) {
    _segmentName = segmentName;
    return this;
  }

  @Nullable
  public String getInstanceName() {
    return _instanceName;
  }

  public PinotControllerJobMetadataDto setInstanceName(@Nullable String instanceName) {
    _instanceName = instanceName;
    return this;
  }
}
