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

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;


public class LArrayPinotBufferFactory implements PinotBufferFactory {
  @Override
  public PinotDataBuffer allocateDirect(long size, ByteOrder byteOrder) {
    if (byteOrder == ByteOrder.nativeOrder()) {
      return PinotNativeOrderLBuffer.allocateDirect(size);
    } else {
      return PinotNonNativeOrderLBuffer.allocateDirect(size);
    }
  }

  @Override
  public PinotDataBuffer mapFile(File file, boolean readOnly, long offset, long size, ByteOrder byteOrder)
      throws IOException {
    if (byteOrder == ByteOrder.nativeOrder()) {
      return PinotNativeOrderLBuffer.mapFile(file, readOnly, offset, size);
    } else {
      return PinotNonNativeOrderLBuffer.mapFile(file, readOnly, offset, size);
    }
  }
}
