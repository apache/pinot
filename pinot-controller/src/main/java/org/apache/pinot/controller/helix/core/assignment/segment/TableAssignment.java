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
package org.apache.pinot.controller.helix.core.assignment.segment;

import java.util.List;
import java.util.Map;


/// Assignment of a table: the instance state map of every segment, and the partition assignment (the instances of
/// every partition, see [org.apache.pinot.common.assignment.PartitionAssignmentUtils]) for a partition-pinned table.
/// The partition assignment is empty for other tables.
public class TableAssignment {
  private final Map<String, Map<String, String>> _segmentAssignment;
  private final Map<Integer, List<String>> _partitionAssignment;

  public TableAssignment(Map<String, Map<String, String>> segmentAssignment,
      Map<Integer, List<String>> partitionAssignment) {
    _segmentAssignment = segmentAssignment;
    _partitionAssignment = partitionAssignment;
  }

  public Map<String, Map<String, String>> getSegmentAssignment() {
    return _segmentAssignment;
  }

  public Map<Integer, List<String>> getPartitionAssignment() {
    return _partitionAssignment;
  }
}
