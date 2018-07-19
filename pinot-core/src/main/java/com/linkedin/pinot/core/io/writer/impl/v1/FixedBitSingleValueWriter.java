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
package com.linkedin.pinot.core.io.writer.impl.v1;

import com.linkedin.pinot.core.io.util.FixedBitIntReaderWriter;
import com.linkedin.pinot.core.io.writer.SingleColumnSingleValueWriter;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;


public class FixedBitSingleValueWriter implements SingleColumnSingleValueWriter {
  private FixedBitIntReaderWriter dataFileWriter;

  public FixedBitSingleValueWriter(File file, int rows, int columnSizeInBits) throws Exception {
    // Convert to long in order to avoid int overflow
    long length =  ((long) rows * columnSizeInBits + Byte.SIZE - 1) / Byte.SIZE;
    // Backward-compatible: index file is always big-endian
    PinotDataBuffer dataBuffer =
        PinotDataBuffer.mapFile(file, false, 0, length, ByteOrder.BIG_ENDIAN, getClass().getSimpleName());
    dataFileWriter = new FixedBitIntReaderWriter(dataBuffer, rows, columnSizeInBits);
  }

  @Override
  public void close() throws IOException {
    dataFileWriter.close();
  }

  @Override
  public void setChar(int row, char ch) {
    throw new UnsupportedOperationException(
        "Only int data type is supported in fixedbit format");
  }

  @Override
  public void setInt(int row, int i) {
    dataFileWriter.writeInt(row, i);
  }

  @Override
  public void setShort(int row, short s) {
    throw new UnsupportedOperationException(
        "Only int data type is supported in fixedbit format");
  }

  @Override
  public void setLong(int row, long l) {
    throw new UnsupportedOperationException(
        "Only int data type is supported in fixedbit format");

  }

  @Override
  public void setFloat(int row, float f) {
    throw new UnsupportedOperationException(
        "Only int data type is supported in fixedbit format");
  }

  @Override
  public void setDouble(int row, double d) {
    throw new UnsupportedOperationException(
        "Only int data type is supported in fixedbit format");

  }

  @Override
  public void setString(int row, String string) {
    throw new UnsupportedOperationException(
        "Only int data type is supported in fixedbit format");

  }

  @Override
  public void setBytes(int row, byte[] bytes) {
    throw new UnsupportedOperationException(
        "Only int data type is supported in fixedbit format");
  }
}
