/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;


public class FixedByteSingleValueMultiColWriter {
  private int[] columnOffsets;
  private int rows;
  private PinotDataBuffer indexDataBuffer;
  private int rowSizeInBytes;

  public FixedByteSingleValueMultiColWriter(File file, int rows, int cols,
      int[] columnSizes)
      throws IOException {
    this.rows = rows;
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

  public FixedByteSingleValueMultiColWriter(PinotDataBuffer dataBuffer, int rows, int cols,
      int[] columnSizes) {
    this.rows = rows;
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

  public void close() throws IOException {
    this.indexDataBuffer.close();
    this.indexDataBuffer = null;
  }
}
