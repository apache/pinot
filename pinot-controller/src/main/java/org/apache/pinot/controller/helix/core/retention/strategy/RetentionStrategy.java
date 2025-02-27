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
package org.apache.pinot.controller.helix.core.retention.strategy;

import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;


/**
 * Strategy to manage segment retention.
 */
public interface RetentionStrategy {

  /**
   * Returns whether the segment should be purged
   *
   * @param tableNameWithType Table name with type
   * @param segmentZKMetadata Segment ZK metadata
   * @return Whether the segment should be purged
   */
  boolean isPurgeable(String tableNameWithType, SegmentZKMetadata segmentZKMetadata);

  /**
   * Returns whether the segment should be purged based on end time.
   *
   * @param segmentName The name of the segment to check
   * @param tableNameWithType Table name with type
   * @param endTimeMs The end time of the segment in milliseconds
   * @return Whether the segment should be purged
   */
  boolean isPurgeable(String segmentName, String tableNameWithType, long endTimeMs);
  }
