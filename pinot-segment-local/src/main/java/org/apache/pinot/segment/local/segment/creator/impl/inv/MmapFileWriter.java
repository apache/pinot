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
package org.apache.pinot.segment.local.segment.creator.impl.inv;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.Consumer;
import org.apache.pinot.common.utils.FileUtils;
import org.apache.pinot.segment.spi.memory.CleanerUtil;


public class MmapFileWriter implements Closeable {

  private final FileChannel _fileChannel;
  private final MappedByteBuffer _buffer;

  public MmapFileWriter(File file, int size)
      throws IOException {
    _fileChannel = new RandomAccessFile(file, "rw").getChannel();
    _buffer = _fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, size);
  }

  public void write(Consumer<ByteBuffer> writer) {
    writer.accept(_buffer);
  }

  @Override
  public void close()
      throws IOException {
    try {
      FileUtils.forceMappedBuffers(_buffer);
    } finally {
      try {
        FileUtils.syncAndClose(_fileChannel);
      } finally {
        if (CleanerUtil.UNMAP_SUPPORTED) {
          CleanerUtil.BufferCleaner cleaner = CleanerUtil.getCleaner();
          cleaner.freeBuffer(_buffer);
        }
      }
    }
  }
}
