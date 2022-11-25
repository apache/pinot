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
package org.apache.pinot.segment.spi.memory.chronicle;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.MappedBytes;
import org.apache.pinot.segment.spi.memory.OnlyNativePinotBufferFactory;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


public class ChroniclePinotBufferFactory extends OnlyNativePinotBufferFactory {
  @Override
  protected PinotDataBuffer allocateDirect(long size) {
    Bytes<?> store;
    if (size < Integer.MAX_VALUE) {
      store = Bytes.wrapForWrite(ByteBuffer.allocateDirect((int) size));
    } else {
      store = Bytes.allocateDirect(size);
    }
    return new ChronicleDataBuffer(store, true, false, 0, size);
  }

  @Override
  protected PinotDataBuffer mapFile(File file, boolean readOnly, long offset, long size)
      throws IOException {
    MappedBytes mappedBytes = MappedBytes.singleMappedBytes(file, offset + size, readOnly);
    return new ChronicleDataBuffer(mappedBytes, true, true, offset, size + offset);
  }
}
