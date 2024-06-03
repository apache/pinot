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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;


/**
 * SeekableInputStream is a class with the methods needed by Pinot to read data efficiently.
 * <p>
 * This class is based on Parquet's SeekableInputStream.
 */
public abstract class SeekableInputStream extends InputStream {

  /**
   * Return the current position in the InputStream.
   *
   * @return current position in bytes from the start of the stream
   */
  public abstract long getCurrentOffset();

  /**
   * Seek to a new position in the InputStream.
   *
   * @param newPos the new position to seek to
   * @throws IllegalArgumentException If the new position is negative or greater than the length of the stream
   */
  public abstract void seek(long newPos);

  /**
   * Read {@code buf.remaining()} bytes of data into a {@link ByteBuffer}.
   * <p>
   * This method will copy available bytes into the buffer, reading at most
   * {@code buf.remaining()} bytes. The number of bytes actually copied is
   * returned by the method, or -1 is returned to signal that the end of the
   * underlying stream has been reached.
   *
   * @param buf a byte buffer to fill with data from the stream
   * @return the number of bytes read or -1 if the stream ended. It may be 0 if there are no bytes available in the
   *         stream
   * @throws IOException If the underlying stream throws IOException
   */
  public abstract int read(ByteBuffer buf) throws IOException;
}
