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
package org.apache.pinot.segment.local.segment.index.readers.text;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/**
 * Custom Lucene BufferedIndexInput implementation that reads from a PinotDataBuffer.
 * This allows Lucene to read file data directly from memory buffers with buffering for better performance.
 */
public class PinotBufferIndexInput extends BufferedIndexInput {
  private final PinotDataBuffer _buffer;
  private final long _length;

  public PinotBufferIndexInput(String name, PinotDataBuffer buffer, long startOffset, long length) {
    super(name);
    _buffer = buffer.view(startOffset, startOffset + length);
    _length = length;
  }

  @Override
  public void close()
      throws IOException {
    // No-op - buffer is managed externally
  }

  @Override
  protected void readInternal(ByteBuffer b)
      throws IOException {
    long currentPosition = getFilePointer();
    if (currentPosition + b.remaining() > _length) {
      throw new IOException("Read past end of file");
    }

    // Copy data from PinotDataBuffer to ByteBuffer
    byte[] tempBuffer = new byte[b.remaining()];
    _buffer.copyTo(currentPosition, tempBuffer, 0, tempBuffer.length);
    b.put(tempBuffer);
  }

  @Override
  public long length() {
    return _length;
  }

  @Override
  protected void seekInternal(long pos) throws IOException {
    if (pos < 0 || pos > _length) {
      throw new IOException("Seek position out of bounds: " + pos + ", length: " + _length);
    }
    // BufferedIndexInput handles the actual seeking, we just validate bounds
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    if (offset < 0 || length < 0 || offset + length > _length) {
      throw new IOException("Slice out of bounds: offset=" + offset + ", length=" + length + ", fileLength=" + _length);
    }
    PinotDataBuffer sliceBuffer = _buffer.view(offset, offset + length);
    return new PinotBufferIndexInput(getFullSliceDescription(sliceDescription), sliceBuffer, 0, length);
  }

  @Override
  public String toString() {
    return "PinotBufferIndexInput(name=" + super.toString() + ", length=" + _length + ")";
  }
}
