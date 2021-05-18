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
package org.apache.pinot.segment.local.upsert;

import org.apache.pinot.segment.local.realtime.impl.ThreadSafeMutableRoaringBitmap;


/**
 * Indicate a record's location on the local host.
 */
public class RecordLocation {
  private final String _segmentName;
  private final int _docId;
  private final long _timestamp;
  private final ThreadSafeMutableRoaringBitmap _validDocIds;

  public RecordLocation(String segmentName, int docId, long timestamp, ThreadSafeMutableRoaringBitmap validDocIds) {
    _segmentName = segmentName;
    _docId = docId;
    _timestamp = timestamp;
    _validDocIds = validDocIds;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public int getDocId() {
    return _docId;
  }

  public long getTimestamp() {
    return _timestamp;
  }

  public ThreadSafeMutableRoaringBitmap getValidDocIds() {
    return _validDocIds;
  }
}
