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
  private String status;
  private RebalanceStatus statusCode;

  public RebalanceResult() {
  }

  public RebalanceResult(@JsonProperty("idealState") Map<String, Map<String, String>> idealStateMapping,
      @JsonProperty("partitionAssignment") PartitionAssignment partitionAssignment, String status) {
    this.idealStateMapping = idealStateMapping;
    this.partitionAssignment = partitionAssignment;
    this.status = status;
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

  public void setStatus(String status) {
    this.status = status;
  }

  public String getStatus() {
    return status;
  }

  public enum RebalanceStatus {
    DONE, FAILED
  }

  public RebalanceStatus getStatusCode() {
    return statusCode;
  }

  public void setStatusCode(RebalanceStatus status) {
    this.statusCode = status;
  }
}
