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
package org.apache.pinot.segment.local.io.util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.pinot.segment.spi.memory.CleanerUtil;

import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * The value writer for var-length values (STRING and BYTES).
 *
 * The layout of the file is as follows:
 * <p>
 * Header Section:
 * <ul>
 *   <li>
 *     Magic bytes: Chose this to be ".vl;" to avoid conflicts with the fixed size value buffer implementations. By
 *     having special characters, this avoids conflicts with regular bytes/strings dictionaries.
 *   </li>
 *   <li>
 *     Version number: This is an integer and can be used for the evolution of the store implementation by incrementing
 *     version for every incompatible change to the store/format.
 *   </li>
 *   <li>
 *     Number of elements in the store.
 *   </li>
 *   <li>
 *     The offset where the data section starts. Though the data section usually starts right after he header, having
 *     this explicitly will let the store to be evolved freely without any assumptions about where the data section
 *     starts.
 *   </li>
 * </ul>
 * <p>
 * Data section:
 * <ul>
 *   <li>
 *     Offsets Array: Integer offsets of the start position of the byte arrays.
 *     Example: [O(0), O(1),...O(n)] where O is the Offset function.
 *     Length of nth element is computed as: O(n+1) - O(n). Since the last element's length can't be computed using this
 *     formula, we store an extra offset at the end to be able to compute last element's length with the same formula,
 *     without depending on underlying buffer's size.
 *   </li>
 *   <li>
 *     All byte arrays.
 *   </li>
 * </ul>
 *
 * @see FixedByteValueReaderWriter
 */
public class VarLengthValueWriter implements Closeable {

  /**
   * Magic bytes used to identify the dictionary files written in variable length bytes format.
   */
  static final byte[] MAGIC_BYTES = ".vl;".getBytes(UTF_8);

  /**
   * Increment this version if there are any structural changes in the store format and
   * deal with backward compatibility correctly based on old versions.
   */
  static final int VERSION = 1;

  // Offsets of different fields in the header. Having as constants for readability.
  static final int VERSION_OFFSET = MAGIC_BYTES.length;
  static final int NUM_VALUES_OFFSET = VERSION_OFFSET + Integer.BYTES;
  static final int DATA_SECTION_OFFSET_POSITION = NUM_VALUES_OFFSET + Integer.BYTES;
  static final int HEADER_LENGTH = DATA_SECTION_OFFSET_POSITION + Integer.BYTES;

  private final FileChannel _fileChannel;
  private final ByteBuffer _offsetBuffer;
  private final ByteBuffer _valueBuffer;

  public VarLengthValueWriter(File outputFile, int numValues)
      throws IOException {
    _fileChannel = new RandomAccessFile(outputFile, "rw").getChannel();
    _offsetBuffer = _fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE);

    // Write the header
    _offsetBuffer.put(MAGIC_BYTES);
    _offsetBuffer.putInt(VERSION);
    _offsetBuffer.putInt(numValues);
    _offsetBuffer.putInt(HEADER_LENGTH);

    _valueBuffer = _offsetBuffer.duplicate();
    _valueBuffer.position(HEADER_LENGTH + (numValues + 1) * Integer.BYTES);
  }

  public void add(byte[] value)
      throws IOException {
    add(value, value.length);
  }

  public void add(byte[] value, int length)
      throws IOException {
    _offsetBuffer.putInt(_valueBuffer.position());
    _valueBuffer.put(value, 0, length);
  }

  @Override
  public void close()
      throws IOException {
    int fileLength = _valueBuffer.position();
    _offsetBuffer.putInt(fileLength);
    _fileChannel.truncate(fileLength);
    _fileChannel.close();
    if (CleanerUtil.UNMAP_SUPPORTED) {
      CleanerUtil.BufferCleaner cleaner = CleanerUtil.getCleaner();
      cleaner.freeBuffer(_offsetBuffer);
    }
  }
}
