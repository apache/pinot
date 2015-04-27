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

import java.io.File;

import com.linkedin.pinot.core.index.reader.DataFileMetadata;
import com.linkedin.pinot.core.index.writer.SingleColumnSingleValueWriter;

public class DefaultSingleColumnSingleValueWriter implements
    SingleColumnSingleValueWriter {

  private FixedByteWidthRowColDataFileWriter dataFileWriter;

  public DefaultSingleColumnSingleValueWriter(File file, int rows,
      int columnSizeInBytes) throws Exception {
    dataFileWriter = new FixedByteWidthRowColDataFileWriter(file, rows, 1,
        new int[] { columnSizeInBytes });
  }

  @Override
  public boolean setMetadata(DataFileMetadata metadata) {
    return false;
  }

  @Override
  public void close() {
    dataFileWriter.close();
  }

  @Override
  public void setChar(int row, char ch) {
    dataFileWriter.setChar(row, 0, ch);
  }

  @Override
  public void setInt(int row, int i) {
    dataFileWriter.setInt(row, 0, i);
  }

  @Override
  public void setShort(int row, short s) {
    dataFileWriter.setShort(row, 0, s);
  }

  @Override
  public void setLong(int row, int l) {
    dataFileWriter.setLong(row, 0, l);

  }

  @Override
  public void setFloat(int row, float f) {
    dataFileWriter.setFloat(row, 0, f);
  }

  @Override
  public void setDouble(int row, double d) {
    dataFileWriter.setDouble(row, 0, d);

  }

  @Override
  public void setString(int row, String string) throws Exception {
    dataFileWriter.setString(row, 0, string);

  }

  @Override
  public void setBytes(int row, byte[] bytes) {
    dataFileWriter.setBytes(row, 0, bytes);
  }
}
