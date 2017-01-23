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
package com.linkedin.pinot.core.io.writer.impl.v1;

import java.io.File;

import com.linkedin.pinot.core.io.writer.SingleColumnSingleValueWriter;
import com.linkedin.pinot.core.io.writer.impl.FixedByteSingleValueMultiColWriter;

public class FixedByteSingleValueWriter implements
    SingleColumnSingleValueWriter {
  private FixedByteSingleValueMultiColWriter dataFileWriter;

  public FixedByteSingleValueWriter(File file, int rows,
      int columnSizeInBytes) throws Exception {
    dataFileWriter = new FixedByteSingleValueMultiColWriter(file, rows, 1,
        new int[] { columnSizeInBytes });
  }

  @Override
  public void close() {
    dataFileWriter.close();
  }

  @Override
  public void setChar(int row, char ch) {
    throw new UnsupportedOperationException(
        "Only int data type is supported in fixedbit format");
  }

  @Override
  public void setInt(int row, int i) {
    dataFileWriter.setInt(row, 0, i);
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
