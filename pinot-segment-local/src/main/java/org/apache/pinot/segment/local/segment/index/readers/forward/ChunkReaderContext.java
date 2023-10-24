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
package org.apache.pinot.segment.local.segment.index.readers.forward;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.CleanerUtil;


/**
 * Context for the chunk-based forward index readers.
 * <p>Information saved in the context can be used by subsequent reads as cache:
 * <ul>
 *   <li>
 *     Chunk Buffer from the previous read. Useful if the subsequent read is from the same buffer, as it avoids extra
 *     chunk decompression.
 *   </li>
 *   <li>Id for the chunk</li>
 * </ul>
 */
public class ChunkReaderContext implements ForwardIndexReaderContext {
  @Getter
  private final ByteBuffer _chunkBuffer;

  @Getter
  @Setter
  private int _chunkId;

  @Getter
  @Setter
  private List<ForwardIndexReader.ByteRange> _ranges;

  public ChunkReaderContext(int maxChunkSize) {
    _chunkBuffer = ByteBuffer.allocateDirect(maxChunkSize);
    _chunkId = -1;
    _ranges = new ArrayList<>();
  }

  @Override
  public void close()
      throws IOException {
    if (CleanerUtil.UNMAP_SUPPORTED) {
      CleanerUtil.getCleaner().freeBuffer(_chunkBuffer);
    }
  }
}
