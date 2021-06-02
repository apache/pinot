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
package org.apache.pinot.plugin.minion.tasks.merge_rollup;

public class MergeProperties {
  private final String _mergeType;
  private final long _bufferTimeMs;
  private final long _maxNumRecordsPerSegment;
  private final long _maxNumRecordsPerTask;

  public MergeProperties(String mergeType, long bufferTimeMs, long maxNumRecordsPerSegment,
      long maxNumRecordsPerTask) {
    _mergeType = mergeType;
    _bufferTimeMs = bufferTimeMs;
    _maxNumRecordsPerSegment = maxNumRecordsPerSegment;
    _maxNumRecordsPerTask = maxNumRecordsPerTask;
  }

  public String getMergeType() {
    return _mergeType;
  }

  public long getBufferTimeMs() {
    return _bufferTimeMs;
  }

  public long getMaxNumRecordsPerSegment() {
    return _maxNumRecordsPerSegment;
  }

  public long getMaxNumRecordsPerTask() {
    return _maxNumRecordsPerTask;
  }
}
