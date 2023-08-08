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
package org.apache.pinot.segment.spi.memory;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;


public class ByteBufferPinotBufferFactory implements PinotBufferFactory {
  @Override
  public PinotDataBuffer allocateDirect(long size, ByteOrder byteOrder) {
    Preconditions.checkArgument(size <= Integer.MAX_VALUE,
        "Trying to allocate %s bytes when max is %s", size, Integer.MAX_VALUE);
    return PinotByteBuffer.allocateDirect((int) size, byteOrder);
  }

  @Override
  public PinotDataBuffer readFile(File file, long offset, long size, ByteOrder byteOrder)
      throws IOException {
    Preconditions.checkArgument(size <= Integer.MAX_VALUE,
        "Trying to allocate %s bytes when max is %s", size, Integer.MAX_VALUE);
    return PinotByteBuffer.loadFile(file, offset, (int) size, byteOrder);
  }

  @Override
  public PinotDataBuffer mapFile(File file, boolean readOnly, long offset, long size, ByteOrder byteOrder)
      throws IOException {
    Preconditions.checkArgument(size <= Integer.MAX_VALUE,
        "Trying to allocate {} bytes when max is {}", size, Integer.MAX_VALUE);
    return PinotByteBuffer.mapFile(file, readOnly, offset, (int) size, byteOrder);
  }
}
