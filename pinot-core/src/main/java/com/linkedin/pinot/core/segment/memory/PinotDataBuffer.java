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
package com.linkedin.pinot.core.segment.memory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.segment.ReadMode;


/**
 * In-memory byte buffer for pinot data.
 *
 * The byte buffer may be memory mapped or off-heap (direct allocation).
 * The main advantage of this class over ByteBuffer is to support buffers
 * larger than 2GB. This also allows memory-mapping files larger than 2GB.
 *
 * <b>Thread Safety:</b> PinotDataBuffer is not thread-safe. External synchronization
 * is required for thread-safety.
 *
 * <b>Unchecked:</b> All the acesses to this buffer are unchecked. Meaning, accessing
 * index beyond the size of the buffer is undefined - it may crash or provide garbage
 * value.
 *
 * The 'Index' part of the name is temporary to limit the usage scope
 * in order to bake the class first. Use this as an interface only. It's
 * implementation *will* change.
 *
 */
public abstract class PinotDataBuffer implements AutoCloseable {

  private static boolean USE_LBUFFER = false;
  protected boolean owner;
  /**
   * Fully load the file in to the in-memory buffer
   * @param file file containing index data
   * @param readMode mmap vs heap mode for the buffer
   * @param openMode read or read_write mode for the index
   * @param context context for buffer allocation. Use mainly for resource tracking
   * @return in-memory buffer containing data
   */
  public static PinotDataBuffer fromFile(File file, ReadMode readMode, FileChannel.MapMode openMode, String context)
      throws IOException {
    return fromFile(file, 0, file.length(), readMode, openMode, context);
  }

  /**
   * Loads a portion of file in memory. This will load data from [startPosition, startPosition + length).
   * @param file file to load
   * @param startPosition (inclusive) start startPosition to the load the data from in the file
   * @param length size of the data from
   * @param readMode mmap vs heap
   * @param openMode read vs read/write
   * @param context context for buffer allocation. Use mainly for resource tracking
   * @return in-memory buffer containing data
   * @throws IOException
   */

  public static PinotDataBuffer fromFile(File file, long startPosition, long length,
      ReadMode readMode, FileChannel.MapMode openMode, String context)
      throws IOException {
    Preconditions.checkNotNull(file, "Index file can not be null");
    if (readMode == ReadMode.heap) {
      return loadFromFile(file, startPosition, length, context);
    } else if (readMode == ReadMode.mmap) {
      return mapFromFile(file, startPosition, length, openMode, context);
    } else {
      throw new RuntimeException("Unknown readmode: " + readMode.name());
    }
  }

  private static PinotDataBuffer mapFromFile(File file, long startPosition, long length, FileChannel.MapMode openMode,
      String context)
      throws IOException {
    if (USE_LBUFFER) {
      return PinotLByteBuffer.mapFromFile(file, startPosition, length, openMode, context);
    } else {
      return PinotByteBuffer.mapFromFile(file, startPosition, length, openMode, context);
    }
  }

  private static PinotDataBuffer loadFromFile(File file, long startPosition, long length, String context)
      throws IOException {
    if (USE_LBUFFER) {
      return PinotLByteBuffer.loadFromFile(file, startPosition, length, context);
    } else {
      return PinotByteBuffer.loadFromFile(file, startPosition, length, context);
    }
  }

  public static PinotDataBuffer allocateDirect(long size) {
    return allocateDirect(size, " direct allocation");

  }

  public static PinotDataBuffer allocateDirect(long size, String description) {
    if (USE_LBUFFER) {
      return PinotLByteBuffer.allocateDirect(size);
    } else {
      if (description == null || description.length() == 0) {
        description = " no description";
      }
      return PinotByteBuffer.allocateDirect(size, description);
    }
  }

  /**
   * Duplicate the buffer without transfering ownership.
   * The new buffer will share the underlying data buffer (no data copy) and it's bounds.
   * Limit and size for the new buffer are independent of this buffer.
   * @return newly allocated buffer (does not own data)
   */
  public abstract PinotDataBuffer duplicate();

  /**
   * Releases the data buffer if this is owner.
   * Accesses after close() are undefined
   * @throws Exception
   */
  @Override
  public abstract void close();


  /**
   * Read the byte at index
   * @param index position in bytebuffer
   * @return
   */
  public abstract byte getByte(long index);

  public abstract byte getByte(int index);

  public abstract void putByte(long index, byte val);

  public abstract void putChar(long index, char c);

  public abstract char getChar(long index);

  public abstract void putFloat(long index, float v);

  public abstract void putFloat(int index, float value);

  public abstract void putLong(long index, long l1);

  public abstract long getLong(long index);

  public abstract void putLong(int index, long value);

  public abstract int getInt(int index);

  public abstract int getInt(long index);

  public abstract void putInt(int index, int value);

  public abstract double getDouble(long l);

  public abstract void putDouble(long index, double value);

  public abstract short getShort(int index);

  public abstract short getShort(long index);

  public abstract void putShort(int index, short value);

  public abstract void putShort(long index, short value);

  public abstract void putInt(long index, int value);

  public abstract long getLong(int index);

  public abstract float getFloat(int index);

  public abstract float getFloat(long index);

  public abstract void putByte(int index, byte value);

  public abstract void putDouble(int index, double value);

  public abstract double getDouble(int index);

  public abstract char getChar(int index);

  public abstract void putChar(int index, char value);

  /**
   * creates a view on a slice of buffer with range [0, (end-start) ) mapped
   * to [start, end) of the original buffer. New buffer will share the same
   * underlying buffer as the original. Any changes will be visible in the original buffer.
   *
   * There is no data copy
   * @param start start position
   * @param end end position
   * @return non-owning sliced buffer
   */
  public abstract PinotDataBuffer view(long start, long end);

  /**
   * Copy contents of this buffer from srcOffset to destArray
   * @param srcOffset startPosition in this buffer to copy from
   * @param destArray destination array to copy data to
   * @param destOffset position in destArray to copy from
   * @param size total size of data to copy
   */
  public abstract void copyTo(long srcOffset, byte[] destArray, int destOffset, int size);

  /**
   * Read the given source byte array, then overwrite the buffer contents
   * @param src
   * @param destOffset
   * @return
   */
  public abstract int readFrom(byte[] src, long destOffset);

  /**
   * Read the given source byte arrey, then overwrite the buffer contents
   * @param src
   * @param srcOffset
   * @param destOffset
   * @param length
   * @return
   */
  public abstract int readFrom(byte[] src, int srcOffset, long destOffset, int length);

  public abstract int readFrom(ByteBuffer sourceBuffer, int srcOffset, long destOffset, int length);

  public abstract void readFrom(File dataFile)
      throws IOException;

  protected abstract void readFrom(File file, long startPosition, long length)
      throws IOException;

  public abstract long size();

  /**
   * Optional operation. Returns the raw memory address
   * @return
   */
  public abstract long address();

  /**
   * Gives a ByteBuffer view of the specified range. Writing to the returned ByteBuffer modifies the contenets of this LByteBuffer
   * @param bufferOffset
   * @param size
   * @return
   */
  public abstract ByteBuffer toDirectByteBuffer(long bufferOffset, int size);

  protected abstract long start();

  public abstract void order(ByteOrder byteOrder);
}
