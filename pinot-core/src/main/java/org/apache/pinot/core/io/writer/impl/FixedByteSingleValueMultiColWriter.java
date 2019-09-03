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
package org.apache.pinot.core.io.writer.impl;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


public class FixedByteSingleValueMultiColWriter implements Closeable {
  private final int[] columnOffsets;
  // To deal with a multi-threading scenario in query processing threads
  // (which are currently non-interruptible), a segment could be dropped by
  // the parent thread and the child query thread could still be using
  // segment memory which may have been unmapped depending on when the
  // drop was completed. To protect against this scenario, the data buffer
  // is made volatile and set to null in close() operation after releasing
  // the buffer. This ensures that concurrent thread(s) trying to invoke
  // set**() operations on this class will hit NPE as opposed accessing
  // illegal/invalid memory (which will crash the JVM).
  private volatile PinotDataBuffer indexDataBuffer;
  private int rowSizeInBytes;

  public FixedByteSingleValueMultiColWriter(File file, int rows, int cols, int[] columnSizes)
      throws IOException {
    this.columnOffsets = new int[cols];
    rowSizeInBytes = 0;
    for (int i = 0; i < columnSizes.length; i++) {
      columnOffsets[i] = rowSizeInBytes;
      int colSize = columnSizes[i];
      rowSizeInBytes += colSize;
    }
    int totalSize = rowSizeInBytes * rows;
    // Backward-compatible: index file is always big-endian
    indexDataBuffer =
        PinotDataBuffer.mapFile(file, false, 0, totalSize, ByteOrder.BIG_ENDIAN, getClass().getSimpleName());
  }

  public FixedByteSingleValueMultiColWriter(PinotDataBuffer dataBuffer, int cols, int[] columnSizes) {
    this.columnOffsets = new int[cols];
    rowSizeInBytes = 0;
    for (int i = 0; i < columnSizes.length; i++) {
      columnOffsets[i] = rowSizeInBytes;
      int colSize = columnSizes[i];
      rowSizeInBytes += colSize;
    }
    indexDataBuffer = dataBuffer;
  }

  public boolean open() {
    return true;
  }

  public void setChar(int row, int col, char ch) {
    int offset = rowSizeInBytes * row + columnOffsets[col];
    indexDataBuffer.putChar(offset, ch);
  }

  public void setInt(int row, int col, int i) {
    int offset = rowSizeInBytes * row + columnOffsets[col];
    indexDataBuffer.putInt(offset, i);
  }

  public void setShort(int row, int col, short s) {
    int offset = rowSizeInBytes * row + columnOffsets[col];
    indexDataBuffer.putShort(offset, s);
  }

  public void setLong(int row, int col, long l) {
    int offset = rowSizeInBytes * row + columnOffsets[col];
    indexDataBuffer.putLong(offset, l);
  }

  public void setFloat(int row, int col, float f) {
    int offset = rowSizeInBytes * row + columnOffsets[col];
    indexDataBuffer.putFloat(offset, f);
  }

  public void setDouble(int row, int col, double d) {
    int offset = rowSizeInBytes * row + columnOffsets[col];
    indexDataBuffer.putDouble(offset, d);
  }

  public void setString(int row, int col, String string) {
    setBytes(row, col, StringUtil.encodeUtf8(string));
  }

  public void setBytes(int row, int col, byte[] bytes) {
    int offset = rowSizeInBytes * row + columnOffsets[col];
    indexDataBuffer.readFrom(offset, bytes);
  }

  @Override
  public void close()
      throws IOException {
    if (this.indexDataBuffer != null) {
      this.indexDataBuffer.close();
      this.indexDataBuffer = null;
    }
  }
}
