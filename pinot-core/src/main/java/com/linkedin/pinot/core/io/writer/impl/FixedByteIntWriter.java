/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.io.writer.impl;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.Closeable;


public final class FixedByteIntWriter implements Closeable {
  private static final int INT_SIZE_IN_BYTES = Integer.SIZE / Byte.SIZE;

  private final PinotDataBuffer _dataBuffer;

  public FixedByteIntWriter(PinotDataBuffer dataBuffer, int numValues) {
    Preconditions.checkState(dataBuffer.size() == numValues * INT_SIZE_IN_BYTES);
    _dataBuffer = dataBuffer;
  }

  public void writeInt(int index, int value) {
    _dataBuffer.putInt(index * INT_SIZE_IN_BYTES, value);
  }

  @Override
  public void close() {
    _dataBuffer.close();
  }
}
