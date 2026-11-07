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

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;


/// A big-endian [DataOutput] with a seekable position and bulk writes of primitive arrays.
public abstract class PinotOutputStream extends OutputStream implements DataOutput {

  /// Returns the current position, in bytes from the start of the stream.
  public abstract long getCurrentOffset();

  /// Seeks to a new position.
  ///
  /// @throws IllegalArgumentException if the new position is negative
  public abstract void seek(long newPos);

  @Override
  public void writeBoolean(boolean v)
      throws IOException {
    write(v ? 1 : 0);
  }

  @Override
  public void writeByte(int v)
      throws IOException {
    write(v);
  }

  @Override
  public abstract void writeShort(int v)
      throws IOException;

  @Override
  public void writeChar(int v)
      throws IOException {
    writeShort(v);
  }

  @Override
  public abstract void writeInt(int v)
      throws IOException;

  @Override
  public abstract void writeLong(long v)
      throws IOException;

  @Override
  public void writeFloat(float v)
      throws IOException {
    writeInt(Float.floatToIntBits(v));
  }

  @Override
  public void writeDouble(double v)
      throws IOException {
    writeLong(Double.doubleToLongBits(v));
  }

  /// Writes `length` ints of `values` from `offset`, with the same big-endian bytes as calling [#writeInt(int)] on
  /// each one.
  public abstract void writeInts(int[] values, int offset, int length)
      throws IOException;

  /// Writes `length` longs of `values` from `offset`, with the same big-endian bytes as calling [#writeLong(long)] on
  /// each one.
  public abstract void writeLongs(long[] values, int offset, int length)
      throws IOException;

  /// Writes `length` floats of `values` from `offset` as big-endian raw bits ([Float#floatToRawIntBits]), the same
  /// bytes as `ByteBuffer.putFloat`. Unlike [#writeFloat(float)], a NaN keeps its payload.
  public abstract void writeFloats(float[] values, int offset, int length)
      throws IOException;

  /// Writes `length` doubles of `values` from `offset` as big-endian raw bits ([Double#doubleToRawLongBits]), the same
  /// bytes as `ByteBuffer.putDouble`. Unlike [#writeDouble(double)], a NaN keeps its payload.
  public abstract void writeDoubles(double[] values, int offset, int length)
      throws IOException;

  @Override
  public void writeChars(String s)
      throws IOException {
    for (int i = 0; i < s.length(); i++) {
      writeChar(s.charAt(i));
    }
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

  /// Writes a string as its UTF-8 bytes behind a 4-byte length prefix, the form Pinot commonly uses for strings.
  ///
  /// **Note**: This method is incompatible with [DataOutput#writeUTF(String)]. It is recommended to use this method
  /// instead of the one in [DataOutput] to write strings.
  public void writeInt4String(String v)
      throws IOException {
    byte[] bytes = v.getBytes(StandardCharsets.UTF_8);
    writeInt(bytes.length);
    write(bytes);
  }

  public void write(DataBuffer input)
      throws IOException {
    write(input, 0, input.size());
  }

  /// Writes `length` bytes of `input` from `offset`.
  public abstract void write(DataBuffer input, long offset, long length)
      throws IOException;
}
