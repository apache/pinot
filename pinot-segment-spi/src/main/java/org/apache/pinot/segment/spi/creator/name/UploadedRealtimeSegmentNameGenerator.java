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
import org.apache.commons.lang3.StringUtils;


/**
 * Implementation for generating segment names of the format UploadedRealtimeSegmentName:
 * {prefix}__{tableName}__{partitionId}__{creationTime}__{suffix}
 *
 * <p> Naming convention to represent uploaded segments to a realtime table see UploadedRealtimeSegmentName. The
 * semantic is similar to LLCSegmentName. This naming convention should be preferred when the data is partitioned in
 * generated segments and should be assigned based on partitionId to ensure consistency with stream partitioning for
 * upsert tables.
 */
public class UploadedRealtimeSegmentNameGenerator implements SegmentNameGenerator {

  private static final String DELIMITER = "__";
  private final String _tableName;
  private final int _partitionId;
  private final long _creationTimeMillis;
  private final String _prefix;

  // if suffix is not set then sequenceId is used as segment name suffix
  @Nullable
  private final String _suffix;

  /**
   * Creates a UploadedRealtimeSegmentNameGenerator
   * @param tableName
   * @param partitionId
   * @param creationTimeMillis
   * @param prefix
   * @param suffix optional field for generator, if not specified then sequenceId is used as suffix
   */
  public UploadedRealtimeSegmentNameGenerator(String tableName, int partitionId, long creationTimeMillis, String prefix,
      @Nullable String suffix) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(tableName) && !tableName.contains(DELIMITER) && StringUtils.isNotBlank(prefix)
            && !prefix.contains(DELIMITER), "Invalid tableName or prefix for UploadedRealtimeSegmentNameGenerator");
    Preconditions.checkArgument(creationTimeMillis > 0, "Creation time must be greater than 0");
    if (suffix != null) {
      Preconditions.checkArgument(StringUtils.isNotBlank(suffix) && !suffix.contains(DELIMITER),
          "Invalid suffix for UploadedRealtimeSegmentNameGenerator");
    }
    _tableName = tableName;
    _partitionId = partitionId;
    _creationTimeMillis = creationTimeMillis;
    _prefix = prefix.trim().replaceAll("\\s+", "_");
    _suffix = suffix != null ? suffix.trim().replaceAll("\\s+", "_") : null;
  }

  @Override
  public String generateSegmentName(int sequenceId, @Nullable Object minTimeValue, @Nullable Object maxTimeValue) {
    return Joiner.on(DELIMITER).join(_prefix, _tableName, _partitionId, _creationTimeMillis,
        StringUtils.isBlank(_suffix) ? sequenceId : _suffix);
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder =
        new StringBuilder("UploadedRealtimeSegmentNameGenerator: tableName=").append(_tableName);
    stringBuilder.append(", prefix=").append(_prefix);
    stringBuilder.append(", partitionId=").append(_partitionId);
    if (_suffix != null) {
      stringBuilder.append(", suffix=").append(_suffix);
    }
    stringBuilder.append(", creationTimeMillis=").append(_creationTimeMillis);
    return stringBuilder.toString();
  }
}
