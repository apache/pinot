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

import com.google.common.primitives.Longs;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;


public abstract class PinotOutputStream extends OutputStream implements DataOutput {

  /**
   * Return the current position in the OutputStream.
   *
   * @return current position in bytes from the start of the stream
   */
  public abstract long getCurrentOffset();

  /**
   * Seek to a new position in the OutputStream.
   *
   * @param newPos the new position to seek to
   * @throws IllegalArgumentException If the new position is negative
   */
  public abstract void seek(long newPos);

  /**
   * Moves the current offset, applying the given change.
   * @param change the change to apply to the current offset
   * @throws IllegalArgumentException if the new position is negative
   */
  public void moveCurrentOffset(long change) {
    long newOffset = getCurrentOffset() + change;
    seek(newOffset);
  }

  @Override
  public void writeBoolean(boolean v) throws IOException {
    write(v ? 1 : 0);
  }

  @Override
  public void writeByte(int v) throws IOException {
    write(v);
  }

  @Override
  public void writeChar(int v) throws IOException {
    writeShort(v);
  }

  @Override
  public void writeChars(String s) throws IOException {
    for (int i = 0; i < s.length(); i++) {
      writeChar(s.charAt(i));
    }
  }

  public void writeDouble(double v) throws IOException {
    writeLong(Double.doubleToLongBits(v));
  }

  public void writeFloat(float v) throws IOException {
    writeInt(Float.floatToIntBits(v));
  }

  public void writeInt(int v) throws IOException {
    write(0xFF & (v >> 24));
    write(0xFF & (v >> 16));
    write(0xFF & (v >> 8));
    write(0xFF & v);
  }

  public void writeLong(long v) throws IOException {
    byte[] bytes = Longs.toByteArray(v);
    write(bytes, 0, bytes.length);
  }

  public void writeShort(int v) throws IOException {
    write(0xFF & (v >> 8));
    write(0xFF & v);
  }

  /**
   * This method is commonly used in Pinot to write a string with a 4-byte length prefix.
   * <p>
   * <b>Note</b>: This method is incompatible with {@link DataOutput#writeUTF(String)}. It is recommended to use
   * this method instead of the one in {@link DataOutput} to write strings.
   */
  public void writeInt4String(String v) throws IOException {
    byte[] bytes = v.getBytes(StandardCharsets.UTF_8);
    writeInt(bytes.length);
    write(bytes);
  }

  @Override
  public void writeBytes(String s)
      throws IOException {
    new DataOutputStream(this).writeBytes(s);
  }

  @Override
  public void writeUTF(String s)
      throws IOException {
    new DataOutputStream(this).writeUTF(s);
  }

  public void write(DataBuffer input)
      throws IOException {
    write(input, 0, input.size());
  }

  public void write(DataBuffer input, long offset, long length)
      throws IOException {
    byte[] bytes = new byte[4096];
    long currentOffset = offset;
    while (currentOffset < offset + length) {
      int bytesToRead = (int) Math.min(length - (currentOffset - offset), bytes.length);
      input.copyTo(currentOffset, bytes, 0, bytesToRead);
      write(bytes, 0, bytesToRead);
      currentOffset += bytesToRead;
    }
  }
}
