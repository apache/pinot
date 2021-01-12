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
package org.apache.pinot.spi.stream;

import com.google.common.annotations.VisibleForTesting;

public class LongMsgOffset implements StreamPartitionMsgOffset {
  private final long _offset;

  @VisibleForTesting
  public long getOffset() {
    return _offset;
  }

  public LongMsgOffset(long offset) {
    _offset = offset;
  }

  public LongMsgOffset(String offset) {
    _offset = Long.parseLong(offset);
  }

  public LongMsgOffset(StreamPartitionMsgOffset other) {
    _offset = ((LongMsgOffset)other)._offset;
  }

  @Override
  public int compareTo(Object other) {
    return Long.compare(_offset, ((LongMsgOffset)other)._offset);
  }

  @Override
  public String toString() {
    return Long.toString(_offset);
  }

  @Override
  public String serialize() {
    return toString();
  }

  @Override
  public Checkpoint deserialize(String checkpointStr) {
    return new LongMsgOffset(checkpointStr);
  }
}
