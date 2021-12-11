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
package org.apache.pinot.core.segment.processing.timehandler;

import javax.annotation.Nullable;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Time handler that modifies and partitions the time value based on the epoch time.
 */
public class EpochTimeHandler implements TimeHandler {
  private final String _timeColumn;
  private final DataType _dataType;
  private final DateTimeFormatSpec _formatSpec;
  private final long _startTimeMs;
  private final long _endTimeMs;
  private final long _roundBucketMs;
  private final long _partitionBucketMs;

  public EpochTimeHandler(DateTimeFieldSpec fieldSpec, long startTimeMs, long endTimeMs, long roundBucketMs,
      long partitionBucketMs) {
    _timeColumn = fieldSpec.getName();
    _dataType = fieldSpec.getDataType();
    _formatSpec = new DateTimeFormatSpec(fieldSpec.getFormat());
    _startTimeMs = startTimeMs;
    _endTimeMs = endTimeMs;
    _roundBucketMs = roundBucketMs;
    _partitionBucketMs = partitionBucketMs;
  }

  @Nullable
  public String handleTime(GenericRow row) {
    long timeMs = _formatSpec.fromFormatToMillis(row.getValue(_timeColumn).toString());
    if (_startTimeMs > 0) {
      if (timeMs < _startTimeMs || timeMs >= _endTimeMs) {
        return null;
      }
    }
    if (_roundBucketMs > 0) {
      timeMs = (timeMs / _roundBucketMs) * _roundBucketMs;
      row.putValue(_timeColumn, _dataType.convert(_formatSpec.fromMillisToFormat(timeMs)));
    }
    if (_partitionBucketMs > 0) {
      return Long.toString(timeMs / _partitionBucketMs);
    } else {
      return DEFAULT_PARTITION;
    }
  }
}
