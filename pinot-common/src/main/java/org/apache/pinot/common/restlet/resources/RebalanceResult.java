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
package org.apache.pinot.common.restlet.resources;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import org.apache.pinot.common.partition.PartitionAssignment;


@JsonIgnoreProperties(ignoreUnknown = true)
public class RebalanceResult {
  private Map<String, Map<String, String>> idealStateMapping;
  private PartitionAssignment partitionAssignment;
  private RebalanceStatus status;
  private String statusMessage;

  public RebalanceResult() {
  }

  public RebalanceResult(@JsonProperty("idealState") Map<String, Map<String, String>> idealStateMapping,
      @JsonProperty("partitionAssignment") PartitionAssignment partitionAssignment,
      @JsonProperty("rebalanceStatus") RebalanceStatus status, @JsonProperty("statusMessage") String statusMessage) {
    this.idealStateMapping = idealStateMapping;
    this.partitionAssignment = partitionAssignment;
    this.status = status;
    this.statusMessage = statusMessage;
  }

  public Map<String, Map<String, String>> getIdealStateMapping() {
    return idealStateMapping;
  }

  public void setIdealStateMapping(Map<String, Map<String, String>> idealStateMapping) {
    this.idealStateMapping = idealStateMapping;
  }

  public PartitionAssignment getPartitionAssignment() {
    return partitionAssignment;
  }

  public void setPartitionAssignment(PartitionAssignment partitionAssignment) {
    this.partitionAssignment = partitionAssignment;
  }

  public RebalanceStatus getStatus() {
    return status;
  }

  public void setStatus(RebalanceStatus status) {
    this.status = status;
  }

  public void setStatusMessage(String message) {
    statusMessage = message;
  }

  public String getStatusMessage() {
    return statusMessage;
  }

  public enum RebalanceStatus {
    DONE, FAILED
  }
}
