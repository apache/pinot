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
package org.apache.pinot.spi.config.table.assignment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;


/// Config of how segments are assigned to the instance partitions of an instance partitions type (`OFFLINE` or
/// `COMPLETED`) or a tier, keyed by its name in `segmentAssignmentConfigMap` of the table config. Both fields are
/// optional: the strategy defaults to the one derived from the instance partitions, and the partition column defaults
/// to the single column of the segment partition config.
public class SegmentAssignmentConfig extends BaseJsonConfig {

  @JsonPropertyDescription("Segment assignment strategy: balanced, replicagroup, roundrobin or roundrobinreplicagroup "
      + "(allservers for a dimension table), derived from the instance partitions if not specified")
  @JsonProperty("segmentAssignmentStrategy")
  private final String _assignmentStrategy;

  @JsonPropertyDescription("Column used to resolve the partition of a segment for partition based segment assignment, "
      + "the single column of the segment partition config if not specified")
  private final String _partitionColumn;

  @JsonCreator
  public SegmentAssignmentConfig(@JsonProperty("segmentAssignmentStrategy") @Nullable String assignmentStrategy,
      @JsonProperty("partitionColumn") @Nullable String partitionColumn) {
    _assignmentStrategy = assignmentStrategy;
    _partitionColumn = partitionColumn;
  }

  @Nullable
  public String getAssignmentStrategy() {
    return _assignmentStrategy;
  }

  @Nullable
  public String getPartitionColumn() {
    return _partitionColumn;
  }
}
