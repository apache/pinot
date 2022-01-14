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
package org.apache.pinot.segment.spi.index.reader.provider;

import java.io.IOException;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


public interface BloomFilterReaderProvider {

  /**
   * Creates a {@see BloomFilterReader}
   * @param dataBuffer the buffer, the caller is responsible for closing it
   * @param onHeap whether to duplicate on heap.
   * @return a bloom filter reader
   * @throws IOException if reading from the buffer fails.
   */
  BloomFilterReader newBloomFilterReader(PinotDataBuffer dataBuffer, boolean onHeap)
      throws IOException;
}
