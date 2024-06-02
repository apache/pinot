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
package org.apache.pinot.segment.spi.creator.name;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;


/**
 * Implementation for generating segment names of the format UploadedRealtimeSegmentName:
 * uploaded__{tableName}__{partitionId}__{sequenceId}__{creationTime}__{optionalSuffix}
 *
 * <p>This naming convention is adopted to represent uploaded segments to a realtime table. The semantic is similar
 * to LLCSegmentName. Scenarios where this naming convention can be preferred is:
 * <li> Generating segments from a batch workload
 * <li> Minion based segment transformations
 */
public class UploadedRealtimeSegmentNameGenerator implements SegmentNameGenerator {

  private static final String SEGMENT_NAME_PREFIX = "uploaded";
  private static final String DELIMITER = "__";
  private final String _tableName;
  private final int _partitionId;
  // creation time must be in long and milliseconds since epoch to be consistent with creation.meta time for valid
  // comparison in segment replace flow.
  private final long _creationTimeMillis;
  @Nullable
  private final String _suffix;

  public UploadedRealtimeSegmentNameGenerator(String tableName, int partitionId, long creationTimeMillis,
      String suffix) {
    Preconditions.checkState(creationTimeMillis > 0, "Creation time must be positive");
    Preconditions.checkNotNull(tableName, "Table name cannot be null");
    _tableName = tableName;
    _partitionId = partitionId;
    _creationTimeMillis = creationTimeMillis;
    _suffix = suffix;
  }

  @Override
  public String generateSegmentName(int sequenceId, @Nullable Object minTimeValue, @Nullable Object maxTimeValue) {
    return Joiner.on(DELIMITER).skipNulls()
        .join(SEGMENT_NAME_PREFIX, _tableName, _partitionId, sequenceId, _creationTimeMillis, _suffix);
  }
}
