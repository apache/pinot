/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.MmapUtils;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ByteBuffer that can handle more than 2GB of data.
 * This is implemented as array of ByteBuffers with
 * some extension to serve the widest field (long/double)
 * from the same buffer.
 */
/*
 * Ideally, we should break this down into SingleByteBuffer class
 * that uses just a single byte buffer and HugeByteBuffer class that
 * has an array of ByteBuffers.
 *
 */
public class HugeByteBuffer extends PinotDataBuffer {

  private static Logger LOGGER = LoggerFactory.getLogger(HugeByteBuffer.class);

  private static final int DEFAULT_SEGMENT_SIZE_BYTES = 1024 * 1024 * 1024;

  // 8 means widest field + 1 byte
  private static final int OFF_HEAP_EXTENSION_SIZE_BYTES = 8;

  // 8K...this will be atleast VM.pageSize()
  private static final int MMAP_EXTENSION_SIZE_BYTES = 8 * 1024;

  private ByteBuffer[] buffers;
  private int segmentSize;
  protected long startPosition = 0L;
  protected long size = 0L;

  // for simplicity we allocate extra bytes so that
  // getLong() on the last byte can be returned from
  // the same buffer.
  protected int extensionBytes = 8;

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

    if (readMode == ReadMode.heap) {
      return loadFromFile(file, startPosition, length, openMode, context);
    } else if (readMode == ReadMode.mmap) {
      return mapFromFile(file, startPosition, length, openMode, context);
    } else {
      throw new RuntimeException("Unknown readmode: " + readMode.name());
    }

  }

  static PinotDataBuffer mapFromFile(File file, long start, long length, FileChannel.MapMode openMode, String context)
      throws IOException {
    final int bufSize = DEFAULT_SEGMENT_SIZE_BYTES + MMAP_EXTENSION_SIZE_BYTES;
    ByteBuffer[] buffers = allocBufferArray(length, DEFAULT_SEGMENT_SIZE_BYTES);

    String rafOpenMode = openMode == FileChannel.MapMode.READ_ONLY ? "r" : "rw";
    if (openMode == FileChannel.MapMode.READ_WRITE && !file.exists()) {
      file.createNewFile();
    }

    RandomAccessFile raf = new RandomAccessFile(file, rafOpenMode);
    int bufIndex = 0;
    long pending = length;
    while (pending > 0) {
      int toMap = (int) Math.min(bufSize, pending);
      buffers[bufIndex] = MmapUtils.mmapFile(raf, openMode, start, toMap, file, context);
      start += DEFAULT_SEGMENT_SIZE_BYTES;
      pending -= DEFAULT_SEGMENT_SIZE_BYTES;
      ++bufIndex;
    }

    return new HugeByteBuffer(buffers, 0, length, true /*owner*/, DEFAULT_SEGMENT_SIZE_BYTES, MMAP_EXTENSION_SIZE_BYTES);
  }

  static PinotDataBuffer loadFromFile(File file, long startPosition, long length, FileChannel.MapMode openMode,
      String context)
      throws IOException {
    HugeByteBuffer buffer = allocateDirect(length);
    if (openMode == FileChannel.MapMode.READ_WRITE && !file.exists()) {
      file.createNewFile();
    }

    buffer.readFrom(file, startPosition, length);
    return buffer;
  }

  public static HugeByteBuffer allocateDirect(long size) {
    int bufsize = DEFAULT_SEGMENT_SIZE_BYTES + OFF_HEAP_EXTENSION_SIZE_BYTES;
    ByteBuffer[] buffers = allocBufferArray(size, DEFAULT_SEGMENT_SIZE_BYTES);
    long pending = size;
    for (int i = 0; i < buffers.length; i++) {
      // intentionally checkState rather than for() condition.
      // because failure indicates bad code
      Preconditions.checkState(pending > 0);
      int toAllocate = (int) Math.min(bufsize, pending);
      // TODO: pass in context to the method
      buffers[i] = MmapUtils.allocateDirectByteBuffer(toAllocate, null, "direct huge buffer");
      pending -= (bufsize - OFF_HEAP_EXTENSION_SIZE_BYTES);
    }
    return new HugeByteBuffer(buffers, 0, size, true /*owner*/, DEFAULT_SEGMENT_SIZE_BYTES,
        OFF_HEAP_EXTENSION_SIZE_BYTES);
  }

  private static ByteBuffer[] allocBufferArray(long size, int segmentSize) {
    int bufCount = numBuffers(size, segmentSize);
    return new ByteBuffer[bufCount];
  }

  private static int numBuffers(long size, int segmentSize) {
    return (int) (size / segmentSize) + (size % segmentSize != 0 ? 1 : 0);
  }

  HugeByteBuffer(ByteBuffer[] buffers, long startPosition, long size, boolean owner, int segmentSize, int extensionBytes) {
    this.buffers = buffers;
    this.startPosition = startPosition;
    this.size = size;
    this.owner = owner;
    this.segmentSize = segmentSize;
    this.extensionBytes = extensionBytes;
  }

  @Override
  public PinotDataBuffer duplicate() {
    return new HugeByteBuffer(buffers, startPosition, size, false/*owner*/, segmentSize, extensionBytes);
  }

  @Override
  public void close() {
    if (owner && buffers != null) {
      for (ByteBuffer buffer : buffers) {
        MmapUtils.unloadByteBuffer(buffer);
      }
      buffers = null;
    }
  }

  // we skip index checks on all getters because
  //   1. bytebuffer checks that anyway
  //   2. performance matters
  @Override
  public byte getByte(long index) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    return buffer.get(bo);
  }

  @Override
  public byte getByte(int index) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    return buffer.get(bo);
  }

  @Override
  public void putByte(long index, byte val) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    buffer.put(bo, val);
  }

  @Override
  public void putChar(long index, char c) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    buffer.putChar(bo, c);
  }

  @Override
  public char getChar(long index) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);

    return buffer.getChar(bo);
  }

  @Override
  public void putFloat(long index, float v) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    buffer.putFloat(bo, v);
  }

  @Override
  public void putFloat(int index, float value) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    buffer.putFloat(bo, value);
  }

  @Override
  public void putLong(long index, long val) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    buffer.putLong(bo, val);
  }

  @Override
  public long getLong(long index) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    return buffer.getLong(bo);
  }

  @Override
  public void putLong(int index, long value) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    buffer.putLong(bo, value);
  }

  @Override
  public int getInt(int index) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    return buffer.getInt(bo);
  }

  @Override
  public int getInt(long index) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    return buffer.getInt(bo);
  }

  @Override
  public void putInt(int index, int value) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    buffer.putInt(bo, value);
  }

  @Override
  public double getDouble(long index) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    return buffer.getDouble(bo);
  }

  @Override
  public void putDouble(long index, double value) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    buffer.putDouble(bo, value);
  }

  @Override
  public short getShort(int index) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    return buffer.getShort(bo);
  }

  @Override
  public short getShort(long index) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    return buffer.getShort(bo);
  }

  @Override
  public void putShort(int index, short value) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    buffer.putShort(bo, value);
  }

  @Override
  public void putShort(long index, short value) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    buffer.putShort(bo, value);
  }

  @Override
  public void putInt(long index, int value) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    buffer.putInt(bo, value);
  }

  @Override
  public long getLong(int index) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    return buffer.getLong(index);
  }

  @Override
  public float getFloat(int index) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    return buffer.getFloat(bo);
  }

  @Override
  public float getFloat(long index) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    return buffer.getFloat(bo);
  }

  @Override
  public void putByte(int index, byte value) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    buffer.put(bo, value);
  }

  @Override
  public void putDouble(int index, double value) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    buffer.putDouble(bo, value);
  }

  @Override
  public double getDouble(int index) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    return buffer.getDouble(bo);
  }

  @Override
  public char getChar(int index) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    return buffer.getChar(bo);
  }

  @Override
  public void putChar(int index, char value) {
    ByteBuffer buffer = getBuffer(index);
    int bo = bufferOffset(index);
    buffer.putChar(bo, value);
  }

  @Override
  public HugeByteBuffer view(long start, long end) {
    return new HugeByteBuffer(this.buffers, start() + start, (end-start), false /*ownership*/, segmentSize,
        extensionBytes);
  }

  @Override
  public void copyTo(long srcOffset, byte[] destArray, int destOffset, int size) {
    Preconditions.checkNotNull(destArray);
    Preconditions.checkArgument(srcOffset >= 0);
    Preconditions.checkArgument(destOffset >= 0);
    Preconditions.checkArgument(size >= 0);
    Preconditions.checkArgument(destArray.length - size >= destOffset);
    Preconditions.checkArgument( size <= size() - srcOffset);

    int bufIndex = bufferIndex(srcOffset);
    int bufOffset = bufferOffset(srcOffset);

    int pendingBytes = size;
    while (pendingBytes > 0) {
      ByteBuffer bb = buffers[bufIndex].duplicate();
      bb.position(bufOffset);
      // FIXME: this  can go beyond size()
      int bytesToCopy = Math.min(size, (segmentSize - bufOffset));
      bb.get(destArray, destOffset, bytesToCopy);
      destOffset += bytesToCopy;
      bufOffset = 0;
      pendingBytes -= bytesToCopy;
      ++bufIndex;
    }
  }

  @Override
  public int readFrom(byte[] src, long destOffset) {
    return readFrom(src, 0, destOffset, src.length);
  }

  @Override
  public int readFrom(byte[] src, int srcOffset, long destOffset, int length) {
    int bufIndex = bufferIndex(destOffset);
    int bufPosition = bufferOffset(destOffset);
    int origLength = length;
    while (length > 0) {
      ByteBuffer bb = buffers[bufIndex].duplicate();
      bb.position(bufPosition);
      // FIXME: this can go beyond size()
      int toCopy = Math.min(length, bb.remaining());
      bb.put(src, srcOffset, toCopy);
      if (toCopy == length) {
        length -= toCopy;
        srcOffset += toCopy;
        break;
      }
      length -= (toCopy - extensionBytes);
      srcOffset += (toCopy - extensionBytes);
      bufPosition = 0;
      ++bufIndex;
    }
    return (origLength - length);
  }

  @Override
  public int readFrom(ByteBuffer sourceBuffer, int srcOffset, long destOffset, int length) {
    throw new UnsupportedOperationException("unimplemented");
  }

  @Override
  public void readFrom(File dataFile)
      throws IOException {
    readFrom(dataFile, 0, dataFile.length());
  }

  @Override
  protected void readFrom(File file, long startPosition, long length)
      throws IOException {

    Preconditions.checkNotNull(this.buffers);
    Preconditions.checkState(size() >= (length - startPosition));
    // "r" because this is readFrom
    RandomAccessFile raf = new RandomAccessFile(file, "r");
    FileChannel fileChannel = raf.getChannel();
    int bufIndex = bufferIndex(0);
    int bufPosition = bufferOffset(0);

    while (length > 0) {
      ByteBuffer bb = buffers[bufIndex];
      bb.position(bufPosition);
      fileChannel.position(startPosition);
      int toCopy = (int) Math.min(length, bb.remaining());
      fileChannel.read(bb);
      if (length == toCopy) {
        break;
      }
      startPosition += (toCopy - extensionBytes);
      length -= (toCopy - extensionBytes);
      bufPosition = 0;
      ++bufIndex;
    }
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public long address() {
    return 0;
  }

  // May copy data
  @Override
  public ByteBuffer toDirectByteBuffer(long bufferOffset, int size) {
    if (size > Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("Can not create buffers of size greater than 2GB");
    }

    int startBufIndex = bufferIndex(bufferOffset);
    long endPos = bufferOffset + size;
    int endBufIndex = bufferIndex(endPos);
    int startOffset = bufferOffset(bufferOffset);
    int endOffset = bufferOffset(endPos);

    if (startBufIndex == endBufIndex) {
      ByteBuffer bb = buffers[startBufIndex].duplicate();
      bb.position(startOffset);
      bb.limit(endOffset);
      return bb.slice();
    } else {
      // this can be more than one buffer since our default
      // buffer size if 1/2 of ByteBuffer max
      ByteBuffer toReturn = ByteBuffer.allocateDirect(size);
      toReturn.position(0);
      int pending = size;
      while (startBufIndex <= endBufIndex) {
        ByteBuffer src = buffers[startBufIndex].duplicate();
        src.position(startOffset);
        if (startBufIndex == endBufIndex) {
          src.limit(endOffset);
        }
        toReturn.put(src);
      }
      return toReturn;
    }
  }

  @Override
  protected long start() {
    return startPosition;
  }

  protected ByteBuffer getBuffer(long index) {
    int bi = bufferIndex(index);
    return buffers[bi];
  }

  protected int bufferIndex(long index) {
    return (int) ( (index  + start()) / segmentSize);
  }

  protected int bufferOffset(long index) {
    return (int) ( (index + start()) % segmentSize);
  }

}
