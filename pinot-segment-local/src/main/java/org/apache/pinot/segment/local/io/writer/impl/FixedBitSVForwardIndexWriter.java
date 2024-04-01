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
import org.apache.pinot.segment.local.io.util.FixedBitIntReaderWriter;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/**
 * Bit-compressed dictionary-encoded forward index writer for single-value columns. The values written are dictionary
 * ids.
 */
public class FixedBitSVForwardIndexWriter implements Closeable {
  private final PinotDataBuffer _dataBuffer;
  private final FixedBitIntReaderWriter _intReaderWriter;

  private int _nextDocId = 0;

  public FixedBitSVForwardIndexWriter(File file, int numDocs, int numBitsPerValue)
      throws IOException {
    // Convert to long in order to avoid int overflow
    long length = ((long) numDocs * numBitsPerValue + Byte.SIZE - 1) / Byte.SIZE;
    // Backward-compatible: index file is always big-endian
    _dataBuffer = PinotDataBuffer.mapFile(file, false, 0, length, ByteOrder.BIG_ENDIAN, getClass().getSimpleName());
    _intReaderWriter = new FixedBitIntReaderWriter(_dataBuffer, numDocs, numBitsPerValue);
  }

  public void putDictId(int dictId) {
    _intReaderWriter.writeInt(_nextDocId++, dictId);
  }

  @Override
  public void close()
      throws IOException {
    _intReaderWriter.close();
    _dataBuffer.close();
  }
}
