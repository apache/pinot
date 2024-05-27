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
 * Implementation for generating segment names of the format uploaded_{tableName}_{partitionId}_{sequenceId}_{
 * creationTime}
 */
public class UploadedRealtimeSegmentNameGenerator implements SegmentNameGenerator {

  private static final String SEGMENT_NAME_PREFIX = "uploaded";
  private static final String DELIMITER = "_";
  private final int _partitionId;
  // creation time must be in long and millis to be consistent with creation.meta time for valid comparison in segment
  // replace flow.
  private final long _creationTimeMillis;
  private final String _tableName;

  public UploadedRealtimeSegmentNameGenerator(String tableName, int partitionId, long creationTimeMillis) {
    Preconditions.checkState(creationTimeMillis > 0, "Creation time must be positive");
    Preconditions.checkNotNull(tableName, "Table name cannot be null");
    _tableName = tableName;
    _partitionId = partitionId;
    _creationTimeMillis = creationTimeMillis;
  }

  @Override
  public String generateSegmentName(int sequenceId, @Nullable Object minTimeValue, @Nullable Object maxTimeValue) {
    return Joiner.on(DELIMITER).join(SEGMENT_NAME_PREFIX, _tableName, _partitionId, sequenceId, _creationTimeMillis);
  }
}
