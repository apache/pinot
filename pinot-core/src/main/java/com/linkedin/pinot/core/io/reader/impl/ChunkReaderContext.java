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
package com.linkedin.pinot.core.io.reader.impl;

import java.nio.ByteBuffer;


/**
 * Class to represent the reader context for ChunkReaders.
 * Information saved in the context can be used by subsequent reads as cache.
 * <ul>
 *   <li> Chunk Buffer from the previous read. Useful if the subsequent read is from the same buffer,
 *        as it avoids chunk decompression. </li>
 *   <li> Id for the chunk </li>
 * </ul>
 */
public class ChunkReaderContext extends UnSortedValueReaderContext {
  int _chunkId;
  ByteBuffer _chunkBuffer;

  public ChunkReaderContext(int maxChunkSize) {
    _chunkBuffer = ByteBuffer.allocateDirect(maxChunkSize);
    _chunkId = -1;
  }

  public ByteBuffer getChunkBuffer() {
    return _chunkBuffer;
  }

  public int getChunkId() {
    return _chunkId;
  }

  public void setChunkId(int chunkId) {
    _chunkId = chunkId;
  }
}
