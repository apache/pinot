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
package com.linkedin.pinot.core.startree;

import com.linkedin.pinot.common.data.FieldSpec;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

public class MmapLinkedListStarTreeTable extends LinkedListStarTreeTable {
  private final File backingBufferDir;
  private final int documentIncrement;
  private ByteBuffer backingBuffer;
  private int documentCount;
  private int fileCount;

  public MmapLinkedListStarTreeTable(List<FieldSpec.DataType> dimensionTypes,
                                     List<FieldSpec.DataType> metricTypes,
                                     File backingBufferDir,
                                     int documentIncrement) {
    super(dimensionTypes, metricTypes);
    this.backingBufferDir = backingBufferDir;
    this.documentIncrement = documentIncrement;
    checkBuffer();
  }

  @Override
  protected ByteBuffer toByteBuffer(StarTreeTableRow row) {
    ByteBuffer buffer = getNextBuffer();

    for (int i = 0; i < dimensionTypes.size(); i++) {
      buffer.putInt(row.getDimensions().get(i));
    }

    for (int i = 0; i < metricTypes.size(); i++) {
      switch (metricTypes.get(i)) {
        case SHORT:
          buffer.putShort(row.getMetrics().get(i).shortValue());
          break;
        case INT:
          buffer.putInt(row.getMetrics().get(i).intValue());
          break;
        case LONG:
          buffer.putLong(row.getMetrics().get(i).longValue());
          break;
        case FLOAT:
          buffer.putFloat(row.getMetrics().get(i).floatValue());
          break;
        case DOUBLE:
          buffer.putDouble(row.getMetrics().get(i).doubleValue());
          break;
        default:
          throw new IllegalArgumentException("Unsupported metric type " + metricTypes.get(i));
      }
    }

    return buffer;
  }

  /** Rolls to next buffer if necessary */
  private void checkBuffer() {
    if (backingBuffer == null || backingBuffer.position() == backingBuffer.limit()) {
      try {
        if (!backingBufferDir.exists()) {
          FileUtils.forceMkdir(backingBufferDir);
        }
        File backingBufferFile = new File(backingBufferDir, String.valueOf(fileCount));
        FileChannel backingFileChannel = new RandomAccessFile(backingBufferFile, "rw").getChannel();
        backingBuffer = backingFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, documentIncrement * rowSize);
        fileCount++;
        documentCount = 0;
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private ByteBuffer getNextBuffer() {
    backingBuffer.position(documentCount * rowSize);
    checkBuffer();
    ByteBuffer buffer = backingBuffer.slice();
    buffer.limit(rowSize);
    documentCount++;
    return buffer;
  }
}
