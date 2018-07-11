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

package com.linkedin.pinot.core.startreeV2;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedByteChunkSingleValueReader;


public class DataSource {

  private File _columnDataFile;

  public DataSource(File dataFile) {
    _columnDataFile = dataFile;
  }

  public FixedBitSingleValueReader getDimensionDataSource(int start, int size, int docsCount, int bits)
      throws IOException {
    PinotDataBuffer buffer = PinotDataBuffer.fromFile(_columnDataFile, start, size, ReadMode.mmap, FileChannel.MapMode.READ_WRITE, "testing");
    DataFileReader fwdIndex = new FixedBitSingleValueReader(buffer, docsCount, bits);
    FixedBitSingleValueReader svFwdIndex = (FixedBitSingleValueReader) fwdIndex;

    return svFwdIndex;
  }

  public FixedByteChunkSingleValueReader getMetricDataSource(int start, int size) throws IOException {
    PinotDataBuffer buffer = PinotDataBuffer.fromFile(_columnDataFile, start, size, ReadMode.mmap, FileChannel.MapMode.READ_WRITE, "testing");
    FixedByteChunkSingleValueReader rawIndexReader = new FixedByteChunkSingleValueReader(buffer);

    return rawIndexReader;
  }
}
