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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.segment.ReadMode;
import xerial.larray.buffer.LBuffer;
import xerial.larray.buffer.LBufferAPI;
import xerial.larray.buffer.UnsafeUtil;
import static xerial.larray.buffer.UnsafeUtil.unsafe;
import xerial.larray.mmap.MMapBuffer;
import xerial.larray.mmap.MMapMode;

/**
 * In-memory byte buffer for pinot data.
 *
 * The byte buffer may be memory mapped or off-heap (direct allocation).
 * The main advantage of this class over ByteBuffer is to support buffers
 * larger than 2GB. This also allows memory-mapping files larger than 2GB.
 *
 * <b>NOTE:</b> All the acesses to this buffer are unchecked. Meaning, accessing
 * index beyond the size of the buffer is undefined - it may crash or provide garbage
 * value.
 *
 * The 'Index' part of the name is temporary to limit the usage scope
 * in order to bake the class first. Use this as an interface only. It's
 * implementation *will* change.
 *
 */
public class PinotLByteBuffer extends PinotDataBuffer {
  private static Logger LOGGER = LoggerFactory.getLogger(PinotLByteBuffer.class);

  private LBufferAPI buffer;
  private long startPosition = 0L;
  private long size = 0L;

  /**
   * Fully load the file in to the in-memory buffer
   * @param file file containing index data
   * @param readMode mmap vs heap mode for the buffer
   * @param openMode read or read_write mode for the index
   * @param context context for buffer allocation. Use mainly for resource tracking
   * @return in-memory buffer containing data
   */
  public static PinotLByteBuffer fromFile(File file, ReadMode readMode, FileChannel.MapMode openMode, String context)
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
  public static PinotLByteBuffer fromFile(File file, long startPosition, long length,
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

  static PinotLByteBuffer mapFromFile(File file, long start, long length, FileChannel.MapMode openMode, String context)
      throws IOException {
    MMapBuffer buffer = mapFromFileInternal(file, start, length, openMode, context);
    return new PinotLByteBuffer(buffer, true);
  }

  private static MMapBuffer mapFromFileInternal(File file, long start, long length,
      FileChannel.MapMode openMode, String context)
      throws IOException {
    MMapMode mmapMode = (openMode == FileChannel.MapMode.READ_ONLY) ?
        MMapMode.READ_ONLY : MMapMode.READ_WRITE;
    // TODO: add memory tracking MMapUtils
    MMapBuffer buf = new MMapBuffer(file, start, length, mmapMode);
    return buf;
  }

  static PinotLByteBuffer loadFromFile(File file, long startPosition, long length, String context)
      throws IOException {
    // TODO: track memory
    LBuffer buf = new LBuffer(length);
    PinotLByteBuffer pinotDataBuffer = new PinotLByteBuffer(buf, true);
    pinotDataBuffer.readFrom(file, startPosition, length);
    return pinotDataBuffer;
  }

  public static PinotLByteBuffer allocateDirect(long size) {
    LBuffer buffer = new LBuffer(size);
    PinotLByteBuffer pinotDataBuffer = new PinotLByteBuffer(buffer, true);
    return pinotDataBuffer;
  }

  private PinotLByteBuffer(LBufferAPI buffer, boolean isOwner)  {
    this(buffer, isOwner, 0, buffer.size());
  }

  private PinotLByteBuffer(LBufferAPI buffer, boolean isOwner, long startPosition, long size) {
    Preconditions.checkNotNull(buffer);
    this.buffer = buffer;
    this.owner = isOwner;
    this.startPosition = startPosition;
    this.size = size;
  }

  /**
   * Transfer the ownership of this buffer. Ownership is transferred only if
   * this buffer is the owner. Otherwise, this method simply acts like a copy
   * @param rhs
   */
  public void transferTo(PinotDataBuffer rhs) {
    Preconditions.checkNotNull(rhs);
    Preconditions.checkArgument(rhs instanceof PinotLByteBuffer);
    PinotLByteBuffer rhsBuffer = (PinotLByteBuffer) rhs;
    if (rhs != this) {
      rhsBuffer.buffer = buffer;
      rhsBuffer.owner = owner;
      this.owner = false;
    }
  }

  /**
   * Duplicate this buffer without transferring ownership.
   * The new buffer will share the underlying data buffer (no data copy) and it's bounds.
   * @return newly allocated buffer (does not own data)
   */
  public PinotLByteBuffer duplicate() {
    PinotLByteBuffer duplicate = new PinotLByteBuffer(this.buffer, false, this.startPosition, this.size);
    return duplicate;
  }

  /**
   * Releases the data buffer if this is owner.
   * Accesses after close() are undefined
   * @throws Exception
   */
  @Override
  public void close() {
    if (owner && buffer != null) {
      if (buffer instanceof MMapBuffer) {
        ((MMapBuffer) buffer).flush();
      }

      buffer.release();
      buffer = null;
    }
  }

  /**
   * Returns the byte value at given index
   */
  public byte apply(int index) {
    return buffer.apply(startPosition + index);
  }

  /**
   * Read the byte at index
   * @param index position in bytebuffer
   * @return
   */
  public byte getByte(long index) {
    return buffer.getByte(startPosition + index);
  }
  public byte getByte(int index) {
    return buffer.getByte(startPosition + index);
  }

  public void putByte(long index, byte val) {
    buffer.putByte(startPosition + index, val);
  }

  public void putChar(long index, char c) {
    buffer.putChar(startPosition + index, c);
  }

  public char getChar(long index) {
    return buffer.getChar(startPosition + index);
  }

  public void putFloat(long index, float v) {
    buffer.putFloat(startPosition + index, v);
  }

  public void putFloat(int index, float value) {
    buffer.putFloat(startPosition + index, value);
  }
  public void putLong(long index, long l1) {
    buffer.putLong(startPosition + index, l1);
  }

  public long getLong(long index) {
    return buffer.getLong(startPosition + index);
  }
  public void putLong(int index, long value) {
    buffer.putLong(startPosition + index, value);
  }

  public int getInt(int index) {
    return buffer.getInt(startPosition + index);
  }
  public int getInt(long index) {
    return buffer.getInt(startPosition + index);
  }

  public void putInt(int index, int value) {
    buffer.putInt(startPosition + index, value);
  }

  public double getDouble(long l) {
    return buffer.getDouble(startPosition + l);
  }

  public void putDouble(long index, double value) {
    buffer.putDouble(startPosition + index, value);
  }


  public short getShort(int index) {
    return buffer.getShort(startPosition + index);
  }

  public short getShort(long index) {
    return buffer.getShort(startPosition + index);
  }

  public void putShort(int index, short value) {
    buffer.putShort(startPosition + index, value);
  }

  public void putShort(long index, short value) {
    buffer.putShort(startPosition + index, value);
  }

  public void putInt(long index, int value) {
    buffer.putInt(startPosition + index, value);
  }

  public long getLong(int index) {
    return buffer.getLong(startPosition + index);
  }

  public float getFloat(int index) {
    return buffer.getFloat(startPosition + index);
  }

  public float getFloat(long index) {
    return buffer.getFloat(startPosition + index);
  }

  public void putByte(int index, byte value) {
    buffer.putByte(startPosition + index, value);
  }

  public void putDouble(int index, double value) {
    buffer.putDouble(startPosition + index, value);
  }
  public double getDouble(int index) {
    return buffer.getDouble(startPosition + index);
  }

  public char getChar(int index) {
    return buffer.getChar(startPosition + index);
  }
  public void putChar(int index, char value) {
    buffer.putChar(startPosition + index, value);
  }

  public void fill(long offset, long length, byte value) {
    unsafe.setMemory(address() + offset, length, value);
  }

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
  public PinotLByteBuffer view(long start, long end) {
    PinotLByteBuffer buffer = new PinotLByteBuffer(this.buffer, false, start, (end-start) );
    return buffer;
  }

  /**
   * Copy contents of this buffer from srcOffset to destArray
   * @param srcOffset startPosition in this buffer to copy from
   * @param destArray destination array to copy data to
   * @param destOffset position in destArray to copy from
   * @param size total size of data to copy
   */
  public void copyTo(long srcOffset, byte[] destArray, int destOffset, int size) {
    int cursor = destOffset;
    for (ByteBuffer bb : toDirectByteBuffers(srcOffset, size)) {
      int bbSize = bb.remaining();
      if ((cursor + bbSize) > destArray.length)
        throw new ArrayIndexOutOfBoundsException(String.format("cursor + bbSize = %,d", cursor + bbSize));
      bb.get(destArray, cursor, bbSize);
      cursor += bbSize;
    }
  }

  /**
   * Read the given source byte array, then overwrite the buffer contents
   * @param src
   * @param destOffset
   * @return
   */
  public int readFrom(byte[] src, long destOffset) {
    return readFrom(src, 0, destOffset, src.length);
  }

  /**
   * Read the given source byte arrey, then overwrite the buffer contents
   * @param src
   * @param srcOffset
   * @param destOffset
   * @param length
   * @return
   */
  public int readFrom(byte[] src, int srcOffset, long destOffset, int length) {
    return readFrom(ByteBuffer.wrap(src), srcOffset, destOffset, length);

  }

  public int readFrom(ByteBuffer sourceBuffer, int srcOffset, long destOffset, int length) {
    ByteBuffer dupBuffer = sourceBuffer.duplicate();
    int readLen = (int) Math.min(dupBuffer.limit() - srcOffset, Math.min(size() - destOffset, length));
    ByteBuffer b = toDirectByteBuffer(destOffset, readLen);
    dupBuffer.position(srcOffset);
    // we need to set the limit here (after the Math.min calculation above
    // because of how b.put(dupBuffer) works. it will copy limit() - position()
    // bytes
    dupBuffer.limit(srcOffset + length);
    b.put(dupBuffer);
    return readLen;
  }

  public void readFrom(File dataFile)
      throws IOException {
    readFrom(dataFile, 0, dataFile.length());
  }

  protected void readFrom(File file, long startPosition, long length)
      throws IOException {

    long bufPosition = 0;
    try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
      // arbitrary size..somewhat conservative to avoid impacting
      // jvm configurations
      int readSize = 10 * 1024 * 1024;
      // TODO: track memory
      ByteBuffer readBuffer = ByteBuffer.allocateDirect(readSize);
      long endPosition = startPosition + length;
      for (long offset = startPosition; offset < endPosition; ) {
        int bytesRead = raf.getChannel().read(readBuffer, offset);
        this.readFrom(readBuffer, 0, bufPosition, bytesRead);
        readBuffer.clear();
        bufPosition += bytesRead;
        offset += bytesRead;
      }
    }
  }

  public long size() {
    return size;
  }

  public long address() {
    return buffer.address() + startPosition;
  }

  /**
   * Convert this buffer to a java array.
   * @return
   */
  public byte[] toArray() {
    if (size() > Integer.MAX_VALUE)
      throw new IllegalStateException("Cannot create byte array of more than 2GB");

    int len = (int) size();
    ByteBuffer bb = toDirectByteBuffer(0L, len);
    byte[] b = new byte[len];
    // Copy data to the array
    bb.get(b, 0, len);
    return b;
  }

  /**
   * Gives an sequence of ByteBuffers. Writing to these ByteBuffers modifies the contents of this LBuffer.
   * @return
   */
  public ByteBuffer[] toDirectByteBuffers() {
    return toDirectByteBuffers(startPosition, size());
  }

  public ByteBuffer[] toDirectByteBuffers(long startOffset, long size) {
    long pos = startPosition + startOffset;
    long blockSize = Integer.MAX_VALUE;
    long limit = pos + size;
    int numBuffers = (int) ((size + (blockSize - 1)) / blockSize);
    ByteBuffer[] result = new ByteBuffer[numBuffers];
    int index = 0;
    while (pos < limit) {
      long blockLength = Math.min(limit - pos, blockSize);
      result[index++] = UnsafeUtil.newDirectByteBuffer(address() + pos, (int) blockLength).order(ByteOrder.nativeOrder());
      pos += blockLength;
    }
    return result;
  }

  /**
   * Gives a ByteBuffer view of the specified range. Writing to the returned ByteBuffer modifies the contenets of this LByteBuffer
   * @param bufferOffset
   * @param size
   * @return
   */
  public ByteBuffer toDirectByteBuffer(long bufferOffset, int size) {
    return UnsafeUtil.newDirectByteBuffer(address() + bufferOffset + startPosition, size);
  }

  @Override
  protected long start() {
    return startPosition;
  }

  @Override
  public void order(ByteOrder byteOrder) {
    throw new UnsupportedOperationException();
  }
}
