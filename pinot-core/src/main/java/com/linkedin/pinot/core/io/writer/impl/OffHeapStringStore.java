/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.io.writer.impl;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;


/**
 * @class OffHeapStringStore
 *
 * An off-heap string store that provides APIs to add a string, retrieve a string, and compare string at an index
 * No verification is made as to whether the string added already exists or not. Strings are stored by
 * copying one 'char' at a time into a CharBuffer, and keeping the offsets in the CharBuffer in another IntBuffer.
 * Empty strings are supported.
 *
 * @note The class is thread-safe for single writer and multiple readers.
 *
 * This class has a list of OffHeapStringStore.Buffer objects. As Buffer objects get filled, new Buffer objects
 * are added to the list. New Buffers objects have twice the capacity of the previous Buffer
 *
 * Within a Buffer object strings are stored as below:
 *
 *                  __________________________________
 *                  |  start offset of string 1      |
 *                  |  start offset of string 2      |
 *                  |        .....                   |
 *                  |  start offset of string N      |
 *                  |                                |
 *                  |         UNUSED                 |
 *                  |                                |
 *                  | STRING N .....                 |
 *                  |          .....                 |
 *                  |          .....                 |
 *                  | STRING N-1                     |
 *                  |          .....                 |
 *                  |          .....                 |
 *                  | STRING 0 .....                 |
 *                  |          .....                 |
 *                  |          .....                 |
 *                  |________________________________|
 *
 *
 * We fill the buffer as follows:
 * - The strings are added from the bottom, each new string appearing nearer to the top of the buffer, leaving no
 *   room between them. Each string is stored as a sequence of 'char' elements. (In Java, each char element takes 2
 *   bytes. A string has string.length() char elements in it).
 *
 * - The start offsets of the strings are added from the top. Each start offset is stored as an integer, taking 4 bytes.
 *
 * Each time we want to add a new string, we check if we have space to add the length of the string, and the string
 * string itself. If we do, then we compute the start offset of the new string as:
 *
 *    new-start-offset = (start offset of prev string added) - (length of this string)
 *
 * The new start offset value is stored in the offset
 *
 *    buffer[numStringsSoFar * 4]
 *
 * and the string itself is stored starting at new-start-offset
 *
 */
public class OffHeapStringStore implements Closeable {
  private static final int START_SIZE = 32 * 1024;

  private static class Buffer implements Closeable {
    private static final int INT_SIZE = V1Constants.Numbers.INTEGER_SIZE;
    private static final int CHAR_SIZE = Character.SIZE / 8;

    private final PinotDataBuffer _pinotDataBuffer;
    private final ByteBuffer _byteBuffer;
    private final int _startIndex;
    private final long _size;

    private int _numStrings = 0;
    private int _availEndOffset;  // Exclusive

    private Buffer(long size, int startIndex) {
      if (size >= Integer.MAX_VALUE) {
        size = Integer.MAX_VALUE - 1;
      }
      _pinotDataBuffer = PinotDataBuffer.allocateDirect(size);
      _pinotDataBuffer.order(ByteOrder.nativeOrder());
      _byteBuffer = _pinotDataBuffer.toDirectByteBuffer(0, (int) size);
      _startIndex = startIndex;
      _availEndOffset = _byteBuffer.capacity();
      _size = size;
    }

    private int add(String string) {
      int startOffset = _availEndOffset - string.length() * CHAR_SIZE;
      if (startOffset <= (_numStrings + 1) * INT_SIZE) {
        // full
        return -1;
      }
      for (int i = 0, j = startOffset; i < string.length(); i++, j = j + CHAR_SIZE) {
        _byteBuffer.putChar(j, string.charAt(i));
      }
      _byteBuffer.putInt(_numStrings * INT_SIZE, startOffset);
      _availEndOffset = startOffset;
      return _numStrings++;
    }

    private boolean equalsStringAt(String string, int index) {
      int startOffset = _byteBuffer.getInt(index * INT_SIZE);
      int endOffset = _byteBuffer.capacity();
      if (index > 0) {
        endOffset = _byteBuffer.getInt((index - 1) * INT_SIZE);
      }
      if ((endOffset - startOffset)/CHAR_SIZE != string.length()) {
        return false;
      }
      for (int i = 0, j = startOffset; i < string.length(); i++, j = j + CHAR_SIZE) {
        if (string.charAt(i) != _byteBuffer.getChar(j)) {
          return false;
        }
      }
      return true;
    }

    private String get(final int index) {
      int startOffset = _byteBuffer.getInt(index * INT_SIZE);
      int endOffset = _byteBuffer.capacity();
      if (index > 0) {
        endOffset = _byteBuffer.getInt((index - 1) * INT_SIZE);
      }
      char[] chars = new char[(endOffset - startOffset)/CHAR_SIZE];
      for (int i = 0, j = startOffset; i < chars.length; i++, j = j + CHAR_SIZE) {
        chars[i] = _byteBuffer.getChar(j);
      }
      return new String(chars);
    }

    @Override
    public void close()
        throws IOException {
      _pinotDataBuffer.close();
    }

    private long getSize() {
      return _size;
    }

    private int getStartIndex() {
      return _startIndex;
    }
  }

  private volatile List<Buffer> _buffers = new ArrayList<Buffer>();
  private int _numElements = 0;
  private volatile Buffer _currentBuffer;

  public OffHeapStringStore() {
    expand(START_SIZE);
  }

  // Expand the buffer size, allocating a min of 32k for strings.
  private Buffer expand(long size) {
    Buffer buffer = new Buffer(size, _numElements);
    List<Buffer> newList = new LinkedList<>();
    for (Buffer b : _buffers) {
      newList.add(b);
    }
    newList.add(buffer);
    _buffers = newList;
    _currentBuffer = buffer;
    return buffer;
  }

  private Buffer expand() {
    Buffer lastBuffer = _buffers.get(_buffers.size()-1);
    Buffer newBuffer = expand(lastBuffer.getSize());
    return newBuffer;
  }

  // Returns a string, given an index
  public String get(int index) {
    List<Buffer> bufList = _buffers;
    for (int x = bufList.size()-1; x >= 0; x--) {
      Buffer buffer = bufList.get(x);
      if (index >= buffer.getStartIndex()) {
        return buffer.get(index - buffer.getStartIndex());
      }
    }
    // Assumed that we will never ask for an index that does not exist.
    throw new RuntimeException("dictionary ID '" + index + "' too low");
  }

  // Adds a string and returns the index. No verification is made as to whether the string already exists or not
  public int add(String string) {
    Buffer buffer = _currentBuffer;
    int index = buffer.add(string);
    while (index < 0) {
      buffer = expand();
      index = buffer.add(string);
    }
    _numElements++;
    return index + buffer.getStartIndex();
  }

  public boolean equalsStringAt(String string, int index) {
    List<Buffer> bufList = _buffers;
    for (int x = bufList.size()-1; x >= 0; x--) {
      Buffer buffer = bufList.get(x);
      // Assumed that we will never ask for a
      if (index >= buffer.getStartIndex()) {
        return buffer.equalsStringAt(string, index- buffer.getStartIndex());
      }
    }
    throw new RuntimeException("dictionary ID '" + index + "' too low");
  }

  @Override
  public void close() throws IOException {
    _numElements = 0;
    for (Buffer buffer : _buffers) {
      buffer.close();
    }
    _buffers.clear();
  }
}
