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


/**
 * A factory that receives two delegates, using one when the requested buffer can be indexes with integers and the other
 * in the other case.
 *
 * This is commonly used to use ByteBuffers when possible. The utility of that is questionable, as it can increase the
 * number of megamorphic calls in the hot path and also make errors related with -XX:MaxDirectMemorySize more
 * indeterministic. But it is also the default behavior of Pinot, so it is kept as the default for compatibility
 * reasons.
 */
public class SmallWithFallbackPinotBufferFactory implements PinotBufferFactory {
  private final PinotBufferFactory _small;
  private final PinotBufferFactory _fallback;

  public SmallWithFallbackPinotBufferFactory(PinotBufferFactory small, PinotBufferFactory fallback) {
    _small = small;
    _fallback = fallback;
  }

  @Override
  public PinotDataBuffer allocateDirect(long size, ByteOrder byteOrder) {
    if (size <= Integer.MAX_VALUE) {
      return _small.allocateDirect(size, byteOrder);
    } else {
      return _fallback.allocateDirect(size, byteOrder);
    }
  }

  @Override
  public PinotDataBuffer readFile(File file, long offset, long size, ByteOrder byteOrder)
      throws IOException {
    if (size <= Integer.MAX_VALUE) {
      return _small.readFile(file, offset, size, byteOrder);
    } else {
      return _fallback.readFile(file, offset, size, byteOrder);
    }
  }

  @Override
  public PinotDataBuffer mapFile(File file, boolean readOnly, long offset, long size, ByteOrder byteOrder)
      throws IOException {
    if (size <= Integer.MAX_VALUE) {
      return _small.mapFile(file, readOnly, offset, size, byteOrder);
    } else {
      return _fallback.mapFile(file, readOnly, offset, size, byteOrder);
    }
  }
}
