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
package org.apache.pinot.segment.local.utils.nativefst;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;


/**
 * Standard FST file header, as described in <code>FST</code> package
 * documentation.
 */
public final class FSTHeader {
  /**
   * FST magic (4 bytes).
   */
  final static int FST_MAGIC = 
      ('\\' << 24) | 
      ('f'  << 16) | 
      ('s'  << 8)  | 
      ('a');

  /**
   * Maximum length of the header block.
   */
  static final int MAX_HEADER_LENGTH = 4 + 8;

  /** FST version number. */
  final byte version;

  FSTHeader(byte version) {
    this.version = version;
  }

  /**
   * Read FST header and version from a stream, consuming read bytes.
   * 
   * @param in The input stream to read data from.
   * @return Returns a valid {@link FSTHeader} with version information.
   * @throws IOException If the stream ends prematurely or if it contains invalid data.
   */
  public static FSTHeader read(InputStream in) throws IOException {
    if (in.read() != ((FST_MAGIC >>> 24)       ) ||
        in.read() != ((FST_MAGIC >>> 16) & 0xff) ||
        in.read() != ((FST_MAGIC >>>  8) & 0xff) ||
        in.read() != ((FST_MAGIC       ) & 0xff)) {
      throw new IOException("Invalid file header, probably not an FST.");
    }

    int version = in.read();
    if (version == -1) {
      throw new IOException("Truncated file, no version number.");
    }

    return new FSTHeader((byte) version);
  }

  /**
   * Writes FST magic bytes and version information.
   * 
   * @param os The stream to write to.
   * @param version Automaton version.
   * @throws IOException Rethrown if writing fails.
   */
  public static void write(OutputStream os, byte version) throws IOException {
    os.write(FST_MAGIC >> 24);
    os.write(FST_MAGIC >> 16);
    os.write(FST_MAGIC >>  8);
    os.write(FST_MAGIC);
    os.write(version);
  }
}
