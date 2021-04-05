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
package org.apache.pinot.segment.local.io.writer.impl;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.segment.local.segment.memory.PinotDataBuffer;


public class FixedByteSingleValueMultiColWriter implements Closeable {
  private final int[] _columnOffsets;
  private final int _rowSizeInBytes;
  private final PinotDataBuffer _dataBuffer;
  private final boolean _shouldCloseDataBuffer;

  public FixedByteSingleValueMultiColWriter(File file, int rows, int cols, int[] columnSizes)
      throws IOException {
    _columnOffsets = new int[cols];
    int rowSizeInBytes = 0;
    for (int i = 0; i < cols; i++) {
      _columnOffsets[i] = rowSizeInBytes;
      int colSize = columnSizes[i];
      rowSizeInBytes += colSize;
    }
    _rowSizeInBytes = rowSizeInBytes;
    int totalSize = rowSizeInBytes * rows;
    // Backward-compatible: index file is always big-endian
    _dataBuffer = PinotDataBuffer.mapFile(file, false, 0, totalSize, ByteOrder.BIG_ENDIAN, getClass().getSimpleName());
    _shouldCloseDataBuffer = true;
  }

  public FixedByteSingleValueMultiColWriter(PinotDataBuffer dataBuffer, int cols, int[] columnSizes) {
    _columnOffsets = new int[cols];
    int rowSizeInBytes = 0;
    for (int i = 0; i < cols; i++) {
      _columnOffsets[i] = rowSizeInBytes;
      int colSize = columnSizes[i];
      rowSizeInBytes += colSize;
    }
    _rowSizeInBytes = rowSizeInBytes;
    _dataBuffer = dataBuffer;
    // For passed in PinotDataBuffer, the caller is responsible of closing the PinotDataBuffer.
    _shouldCloseDataBuffer = false;
  }

  @Override
  public void close()
      throws IOException {
    if (_shouldCloseDataBuffer) {
      _dataBuffer.close();
    }
  }

  public boolean open() {
    return true;
  }

  public void setChar(int row, int col, char ch) {
    int offset = _rowSizeInBytes * row + _columnOffsets[col];
    _dataBuffer.putChar(offset, ch);
  }

  public void setInt(int row, int col, int i) {
    int offset = _rowSizeInBytes * row + _columnOffsets[col];
    _dataBuffer.putInt(offset, i);
  }

  public void setShort(int row, int col, short s) {
    int offset = _rowSizeInBytes * row + _columnOffsets[col];
    _dataBuffer.putShort(offset, s);
  }

  public void setLong(int row, int col, long l) {
    int offset = _rowSizeInBytes * row + _columnOffsets[col];
    _dataBuffer.putLong(offset, l);
  }

  public void setFloat(int row, int col, float f) {
    int offset = _rowSizeInBytes * row + _columnOffsets[col];
    _dataBuffer.putFloat(offset, f);
  }

  public void setDouble(int row, int col, double d) {
    int offset = _rowSizeInBytes * row + _columnOffsets[col];
    _dataBuffer.putDouble(offset, d);
  }

  public void setString(int row, int col, String string) {
    setBytes(row, col, StringUtil.encodeUtf8(string));
  }

  public void setBytes(int row, int col, byte[] bytes) {
    int offset = _rowSizeInBytes * row + _columnOffsets[col];
    _dataBuffer.readFrom(offset, bytes);
  }
}
