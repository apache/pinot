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
package org.apache.pinot.segment.spi.memory.unsafe;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link Memory} that is anonymous and therefore there it is not backed by any file.
 */
public class DirectMemory implements Memory {
  private static final Logger LOGGER = LoggerFactory.getLogger(DirectMemory.class);

  private final long _address;
  private final long _size;
  private volatile boolean _closed = false;

  public DirectMemory(long size) {
    _address = Unsafer.UNSAFE.allocateMemory(size);
    _size = size;

    Unsafer.UNSAFE.setMemory(_address, _size, (byte) 0);
  }

  @Override
  public long getAddress() {
    return _address;
  }

  @Override
  public long getSize() {
    return _size;
  }

  @Override
  public void flush() {
  }

  @Override
  public void close()
      throws IOException {
    if (!_closed) {
      synchronized (this) {
        if (!_closed) {
          Unsafer.UNSAFE.freeMemory(_address);
          _closed = true;
        }
      }
    }
  }

  @Override
  protected void finalize()
      throws Throwable {
    if (!_closed) {
      LOGGER.warn("Mmap section of " + _size + " wasn't explicitly closed");
      close();
    }
    super.finalize();
  }
}
