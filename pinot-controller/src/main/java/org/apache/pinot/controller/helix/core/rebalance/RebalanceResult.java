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
package org.apache.pinot.controller.helix.core.rebalance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;


@JsonIgnoreProperties(ignoreUnknown = true)
public class RebalanceResult {
  private final String _jobId;
  private final Status _status;
  private final Map<InstancePartitionsType, InstancePartitions> _instanceAssignment;
  private final Map<String, InstancePartitions> _tierInstanceAssignment;
  private final Map<String, Map<String, String>> _segmentAssignment;
  private final String _description;

  @JsonCreator
  public RebalanceResult(@JsonProperty(value = "jobId", required = true) String jobId,
      @JsonProperty(value = "status", required = true) Status status,
      @JsonProperty(value = "description", required = true) String description,
      @JsonProperty("instanceAssignment") @Nullable Map<InstancePartitionsType, InstancePartitions> instanceAssignment,
      @JsonProperty("tierInstanceAssignment") @Nullable Map<String, InstancePartitions> tierInstanceAssignment,
      @JsonProperty("segmentAssignment") @Nullable Map<String, Map<String, String>> segmentAssignment) {
    _jobId = jobId;
    _status = status;
    _description = description;
    _instanceAssignment = instanceAssignment;
    _tierInstanceAssignment = tierInstanceAssignment;
    _segmentAssignment = segmentAssignment;
  }

  @JsonProperty
  public String getJobId() {
    return _jobId;
  }

  @JsonProperty
  public Status getStatus() {
    return _status;
  }

  @JsonProperty
  public String getDescription() {
    return _description;
  }

  @JsonProperty
  public Map<InstancePartitionsType, InstancePartitions> getInstanceAssignment() {
    return _instanceAssignment;
  }

  @JsonProperty
  public Map<String, InstancePartitions> getTierInstanceAssignment() {
    return _tierInstanceAssignment;
  }

  @JsonProperty
  public Map<String, Map<String, String>> getSegmentAssignment() {
    return _segmentAssignment;
  }

  public enum Status {
    NO_OP, DONE, FAILED, IN_PROGRESS
  }
}
