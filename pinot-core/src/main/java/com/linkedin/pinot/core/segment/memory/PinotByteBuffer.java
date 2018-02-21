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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.MmapUtils;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PinotByteBuffer extends PinotDataBuffer {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotByteBuffer.class);

  private ByteBuffer buffer;
  private RandomAccessFile raf = null;

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
    return PinotByteBuffer.fromFile(file, 0, file.length(), readMode, openMode, context);
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
      return PinotByteBuffer.loadFromFile(file, startPosition, length, context);
    } else if (readMode == ReadMode.mmap) {
      return PinotByteBuffer.mapFromFile(file, startPosition, length, openMode, context);
    } else {
      throw new RuntimeException("Unknown readmode: " + readMode.name() + ", file: " + file);
    }

  }

  static PinotDataBuffer mapFromFile(File file, long start, long length, FileChannel.MapMode openMode, String context)
      throws IOException {
    // file may not exist if it's opened for writing
    Preconditions.checkNotNull(file);
    Preconditions.checkArgument(start >= 0);
    Preconditions.checkArgument(length >= 0 && length < Integer.MAX_VALUE,
        "Mapping files larger than 2GB is not supported, file: " + file.toString() + ", context: " + context
            + ", length: " + length);
    Preconditions.checkNotNull(context);

    if (openMode == FileChannel.MapMode.READ_ONLY) {
      if (!file.exists()) {
        throw new IllegalArgumentException("File: " + file + " must exist to open in read-only mode");
      }
      if (length > (file.length() - start)) {
        throw new IllegalArgumentException(
            String.format("Mapping limits exceed file size, start: %d, length: %d, file size: %d",
                start, length, file.length() ));
      }
    }


    String rafOpenMode = openMode == FileChannel.MapMode.READ_ONLY ? "r" : "rw";
    RandomAccessFile raf = new RandomAccessFile(file, rafOpenMode);
    ByteBuffer bb = MmapUtils.mmapFile(raf, openMode, start, length, file, context);
    PinotByteBuffer pbb = new PinotByteBuffer(bb, true /*owner*/);
    pbb.raf = raf;
    return pbb;
  }

  static PinotDataBuffer loadFromFile(File file, long startPosition, long length, String context)
      throws IOException {
    Preconditions.checkArgument(length >= 0 && length < Integer.MAX_VALUE);
    Preconditions.checkNotNull(file);
    Preconditions.checkArgument(startPosition >= 0);
    Preconditions.checkNotNull(context);
    Preconditions.checkState(file.exists(), "File: {} does not exist", file);
    Preconditions.checkState(file.isFile(), "File: {} is not a regular file", file);

    PinotByteBuffer buffer = allocateDirect((int)length, file.toString()  + "-" + context);

    buffer.readFrom(file, startPosition, length);
    return buffer;
  }

  public static PinotByteBuffer allocateDirect(long size, String context) {
    Preconditions.checkArgument(size >= 0 && size < Integer.MAX_VALUE, "bad value for size " + size);
    Preconditions.checkNotNull(context);

    ByteBuffer bb = MmapUtils.allocateDirectByteBuffer( (int)size, null, context);
    return new PinotByteBuffer(bb, true/*owner*/);
  }

  // package-private
  PinotByteBuffer(ByteBuffer buffer, boolean ownership) {
    this.buffer = buffer;
    this.owner = ownership;
  }

  @Override
  public byte getByte(long index) {
    throw new UnsupportedOperationException("Long index is not supported");
  }

  @Override
  public byte getByte(int index) {
    return buffer.get(index);
  }

  @Override
  public void putByte(long index, byte val) {
    throw new UnsupportedOperationException("Long index is not supported");
  }

  @Override
  public void putByte(int index, byte value) {
    buffer.put(index, value);
  }

  @Override
  public void putChar(long index, char c) {
    throw new UnsupportedOperationException("Long index is not supported");
  }

  @Override
  public char getChar(long index) {
    throw new UnsupportedOperationException("Long index is not supported");
  }

  @Override
  public char getChar(int index) {
    return buffer.getChar(index);
  }

  @Override
  public void putChar(int index, char value) {
    buffer.putChar(index, value);
  }

  @Override
  public short getShort(int index) {
    return buffer.getShort(index);
  }

  @Override
  public short getShort(long index) {
    throw new UnsupportedOperationException("Long index is not supported");
  }

  @Override
  public void putShort(int index, short value) {
    buffer.putShort(index, value);
  }

  @Override
  public void putShort(long index, short value) {
    throw new UnsupportedOperationException("Long index is not supported");
  }

  @Override
  public int getInt(int index) {
    return buffer.getInt(index);
  }

  @Override
  public int getInt(long index) {
    throw new UnsupportedOperationException("Long index is not supported");
  }

  @Override
  public void putInt(int index, int value) {
    buffer.putInt(index, value);
  }

  @Override
  public void putInt(long index, int value) {
    throw new UnsupportedOperationException("Long index is not supported");
  }

  @Override
  public void putLong(long index, long l1) {
    throw new UnsupportedOperationException("Long index is not supported");
  }

  @Override
  public long getLong(long index) {
    throw new UnsupportedOperationException("Long index is not supported");
  }

  @Override
  public void putLong(int index, long value) {
    buffer.putLong(index, value);
  }

  @Override
  public long getLong(int index) {
    return buffer.getLong(index);
  }


  @Override
  public void putFloat(long index, float v) {
    throw new UnsupportedOperationException("Long index is not supported");
  }

  @Override
  public void putFloat(int index, float value) {
    buffer.putFloat(index, value);
  }

  @Override
  public float getFloat(int index) {
    return buffer.getFloat(index);
  }

  @Override
  public float getFloat(long index) {
    throw new UnsupportedOperationException("Long index is not supported");
  }

  @Override
  public double getDouble(int index) {
    return buffer.getDouble(index);
  }

  @Override
  public double getDouble(long l) {
    throw new UnsupportedOperationException("Long index is not supported");
  }

  @Override
  public void putDouble(int index, double value) {
    buffer.putDouble(index, value);
  }

  @Override
  public void putDouble(long index, double value) {
    throw new UnsupportedOperationException("Long index is not supported");
  }

  @Override
  public PinotDataBuffer view(long start, long end) {
    Preconditions.checkArgument(start >= 0 && start <= buffer.limit(),
        "View start position is not valid, start: %s, end: %s, buffer limit: %s", start, end, buffer.limit());
    Preconditions.checkArgument(end >= start && end <= buffer.limit(),
        "View end position is not valid, start: %s, end: %s, buffer limit: %s", start, end, buffer.limit());

    ByteBuffer bb = this.buffer.duplicate();
    bb.position((int)start);
    bb.limit((int)end);
    return new PinotByteBuffer(bb.slice(), false);
  }

  @Override
  public void copyTo(long srcOffset, byte[] destArray, int destOffset, int size) {
    ByteBuffer srcBB = buffer.duplicate();
    srcBB.position((int)srcOffset);
    srcBB.get(destArray, destOffset, size);
  }

  @Override
  public int readFrom(byte[] src, long destOffset) {
    return readFrom(src, 0, destOffset, src.length);
  }

  @Override
  public int readFrom(byte[] src, int srcOffset, long destOffset, int length) {
    ByteBuffer dup = buffer.duplicate();
    dup.position((int)destOffset);
    dup.put(src, srcOffset, length);
    return length;
  }

  @Override
  public int readFrom(ByteBuffer sourceBuffer, int srcOffset, long destOffset, int length) {
    ByteBuffer srcDup = sourceBuffer.duplicate();
    ByteBuffer localDup = buffer.duplicate();
    srcDup.position(srcOffset);
    srcDup.limit(srcOffset + length);
    localDup.put(srcDup);
    return length;
  }

  @Override
  public void readFrom(File dataFile)
      throws IOException {
    readFrom(dataFile, 0, dataFile.length());
  }

  @Override
  protected void readFrom(File file, long startPosition, long length)
      throws IOException {
    try (RandomAccessFile raf = new RandomAccessFile(file, "r") ) {
      raf.getChannel().position(startPosition);
      ByteBuffer dup = buffer.duplicate();
      dup.position(0);
      dup.limit((int) length);
      raf.getChannel().read(dup, startPosition);
    }
  }

  @Override
  public long size() {
    return buffer.capacity();
  }

  @Override
  public long address() {
    return 0;
  }

  @Override
  public ByteBuffer toDirectByteBuffer(long bufferOffset, int size) {
    ByteBuffer bb = buffer.duplicate();
    bb.position((int)bufferOffset);
    bb.limit((int)bufferOffset + size);
    return bb.slice();
  }

  @Override
  protected long start() {
    return buffer.clear().position();
  }

  @Override
  public void order(ByteOrder byteOrder) {
    buffer.order(byteOrder);
  }

  @Override
  public PinotDataBuffer duplicate() {
    PinotByteBuffer dup = new PinotByteBuffer(this.buffer.duplicate(), false);
    return dup;
  }

  @Override
  public void close() {
    if (!owner || buffer == null) {
      return;
    }
    MmapUtils.unloadByteBuffer(buffer);
    if (raf != null) {
      try {
        raf.close();
      } catch (IOException e) {
        LOGGER.error("Failed to close file: {}. Continuing with errors", raf.toString(), e);
      }
    }

    buffer = null;
    raf = null;
  }
}
