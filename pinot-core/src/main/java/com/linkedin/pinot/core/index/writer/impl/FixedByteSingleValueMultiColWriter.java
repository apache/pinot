/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.index.writer.impl;

import com.linkedin.pinot.common.utils.MmapUtils;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import org.apache.commons.io.IOUtils;


public class FixedByteSingleValueMultiColWriter {
  private File file;
  private int cols;
  private int[] columnOffsets;
  private int rows;
  private ByteBuffer byteBuffer;
  private RandomAccessFile raf;
  private int rowSizeInBytes;
  private final boolean ownsByteBuffer;

  public FixedByteSingleValueMultiColWriter(File file, int rows, int cols,
      int[] columnSizes) throws Exception {
    this.file = file;
    this.rows = rows;
    this.cols = cols;
    this.columnOffsets = new int[cols];
    raf = new RandomAccessFile(file, "rw");
    rowSizeInBytes = 0;
    for (int i = 0; i < columnSizes.length; i++) {
      columnOffsets[i] = rowSizeInBytes;
      int colSize = columnSizes[i];
      rowSizeInBytes += colSize;
    }
    int totalSize = rowSizeInBytes * rows;
    byteBuffer = MmapUtils.mmapFile(raf, FileChannel.MapMode.READ_WRITE, 0,
        totalSize, file, this.getClass().getSimpleName() + " byteBuffer");
    ownsByteBuffer = true;
  }

  public FixedByteSingleValueMultiColWriter(ByteBuffer byteBuffer, int rows,
      int cols, int[] columnSizes) throws IOException {
    this.rows = rows;
    this.cols = cols;
    this.columnOffsets = new int[cols];
    rowSizeInBytes = 0;
    for (int i = 0; i < columnSizes.length; i++) {
      columnOffsets[i] = rowSizeInBytes;
      int colSize = columnSizes[i];
      rowSizeInBytes += colSize;
    }
    this.byteBuffer = byteBuffer;
    ownsByteBuffer = false;
  }

  public boolean open() {
    return true;
  }

  public void setChar(int row, int col, char ch) {
    int offset = rowSizeInBytes * row + columnOffsets[col];
    byteBuffer.putChar(offset, ch);
  }

  public void setInt(int row, int col, int i) {
    int offset = rowSizeInBytes * row + columnOffsets[col];
    byteBuffer.putInt(offset, i);
  }

  public void setShort(int row, int col, short s) {
    int offset = rowSizeInBytes * row + columnOffsets[col];
    byteBuffer.putShort(offset, s);
  }

  public void setLong(int row, int col, long l) {
    int offset = rowSizeInBytes * row + columnOffsets[col];
    byteBuffer.putLong(offset, l);
  }

  public void setFloat(int row, int col, float f) {
    int offset = rowSizeInBytes * row + columnOffsets[col];
    byteBuffer.putFloat(offset, f);
  }

  public void setDouble(int row, int col, double d) {
    int offset = rowSizeInBytes * row + columnOffsets[col];
    byteBuffer.putDouble(offset, d);
  }

  public void setString(int row, int col, String string) {
    setBytes(row, col, string.getBytes(Charset.forName("UTF-8")));
  }

  public void setBytes(int row, int col, byte[] bytes) {
    int offset = rowSizeInBytes * row + columnOffsets[col];
    byteBuffer.position(offset);
    byteBuffer.put(bytes);
  }

  public void close() {
    IOUtils.closeQuietly(raf);
    raf = null;
    if (ownsByteBuffer) {
      MmapUtils.unloadByteBuffer(byteBuffer);
      byteBuffer = null;
    }
  }
}
