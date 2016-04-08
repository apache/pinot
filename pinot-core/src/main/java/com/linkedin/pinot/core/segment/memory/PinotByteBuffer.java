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

import com.linkedin.pinot.common.utils.MmapUtils;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotByteBuffer extends PinotDataBuffer {
  private static Logger LOGGER = LoggerFactory.getLogger(PinotByteBuffer.class);

  private ByteBuffer buffer;

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
  public void putChar(long index, char c) {
    throw new UnsupportedOperationException("Long index is not supported");
  }

  @Override
  public char getChar(long index) {
    throw new UnsupportedOperationException("Long index is not supported");
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
  public double getDouble(long l) {
    throw new UnsupportedOperationException("Long index is not supported");
  }

  @Override
  public void putDouble(long index, double value) {
    throw new UnsupportedOperationException("Long index is not supported");
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
  public void putInt(long index, int value) {
    throw new UnsupportedOperationException("Long index is not supported");
  }

  @Override
  public long getLong(int index) {
    return buffer.getLong(index);
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
  public void putByte(int index, byte value) {
    buffer.put(index, value);
  }

  @Override
  public void putDouble(int index, double value) {
    buffer.putDouble(index, value);
  }

  @Override
  public double getDouble(int index) {
    return buffer.getDouble(index);
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
  public PinotDataBuffer view(long start, long end) {
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
    RandomAccessFile raf = new RandomAccessFile(file, "r");
    raf.getChannel().position(startPosition);
    ByteBuffer dup = buffer.duplicate();
    dup.position(0);
    dup.limit((int)length);
    raf.getChannel().read(dup, startPosition);
  }

  @Override
  public long size() {
    return buffer.capacity();
  }

  @Override
  public long address() {
    return 0;
  }

  //@Override
  public byte[] toArray() {
    throw new UnsupportedOperationException("Unimplemented");
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
  public PinotDataBuffer duplicate() {
    PinotByteBuffer dup = new PinotByteBuffer(this.buffer, false);
    return dup;
  }

  @Override
  public void close() {
    if (owner && buffer != null) {
      MmapUtils.unloadByteBuffer(buffer);
    }
  }
}
