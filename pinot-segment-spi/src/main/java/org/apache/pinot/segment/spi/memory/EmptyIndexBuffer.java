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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Properties;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * A specialized PinotDataBuffer implementation for zero-size index entries that configuration.
 * This buffer is useful for debugging and tracking purposes when dealing with empty index entries.
 */
public class EmptyIndexBuffer extends PinotDataBuffer {
  private final Properties _properties;
  private final String _segmentName;
  private final String _tableNameWithType;

  /**
   * Creates a new EmptyIndexBuffer for a zero-size index entry
   *
   * @param properties Properties containing configuration
   * @param segmentName The name of the segment
   * @param tableNameWithType The table name with type
   */
  public EmptyIndexBuffer(Properties properties, String segmentName, String tableNameWithType) {
    super(false); // Not closeable since it's just metadata
    _properties = properties;
    _segmentName = segmentName;
    _tableNameWithType = tableNameWithType;
  }

  /**
   * Gets the properties containing configuration information
   * @return The properties
   */
  public Properties getProperties() {
    return _properties;
  }

  @Override
  public byte getByte(long offset) {
    throw new UnsupportedOperationException(
        String.format("Cannot read from empty buffer for index: %s, segment: %s, table: %s", _segmentName,
            _tableNameWithType));
  }

  @Override
  public void putByte(long offset, byte value) {
    throw new UnsupportedOperationException(
        String.format("Cannot write to empty buffer for index: %s, segment: %s, table: %s", _segmentName,
            _tableNameWithType));
  }

  @Override
  public char getChar(long offset) {
    throw new UnsupportedOperationException(
        String.format("Cannot read from empty buffer for index: %s, segment: %s, table: %s", _segmentName,
            _tableNameWithType));
  }

  @Override
  public void putChar(long offset, char value) {
    throw new UnsupportedOperationException(
        String.format("Cannot write to empty buffer for index: %s, segment: %s, table: %s", _segmentName,
            _tableNameWithType));
  }

  @Override
  public short getShort(long offset) {
    throw new UnsupportedOperationException(
        String.format("Cannot read from empty buffer for index: %s, segment: %s, table: %s", _segmentName,
            _tableNameWithType));
  }

  @Override
  public void putShort(long offset, short value) {
    throw new UnsupportedOperationException(
        String.format("Cannot write to empty buffer for index: %s, segment: %s, table: %s", _segmentName,
            _tableNameWithType));
  }

  @Override
  public int getInt(long offset) {
    throw new UnsupportedOperationException(
        String.format("Cannot read from empty buffer for index: %s, segment: %s, table: %s", _segmentName,
            _tableNameWithType));
  }

  @Override
  public void putInt(long offset, int value) {
    throw new UnsupportedOperationException(
        String.format("Cannot write to empty buffer for index: %s, segment: %s, table: %s", _segmentName,
            _tableNameWithType));
  }

  @Override
  public long getLong(long offset) {
    throw new UnsupportedOperationException(
        String.format("Cannot read from empty buffer for index: %s, segment: %s, table: %s", _segmentName,
            _tableNameWithType));
  }

  @Override
  public void putLong(long offset, long value) {
    throw new UnsupportedOperationException(
        String.format("Cannot write to empty buffer for index: %s, segment: %s, table: %s", _segmentName,
            _tableNameWithType));
  }

  @Override
  public float getFloat(long offset) {
    throw new UnsupportedOperationException(
        String.format("Cannot read from empty buffer for index: %s, segment: %s, table: %s", _segmentName,
            _tableNameWithType));
  }

  @Override
  public void putFloat(long offset, float value) {
    throw new UnsupportedOperationException(
        String.format("Cannot write to empty buffer for index: %s, segment: %s, table: %s", _segmentName,
            _tableNameWithType));
  }

  @Override
  public double getDouble(long offset) {
    throw new UnsupportedOperationException(
        String.format("Cannot read from empty buffer for index: %s, segment: %s, table: %s", _segmentName,
            _tableNameWithType));
  }

  @Override
  public void putDouble(long offset, double value) {
    throw new UnsupportedOperationException(
        String.format("Cannot write to empty buffer for index: %s, segment: %s, table: %s", _segmentName,
            _tableNameWithType));
  }

  @Override
  public void copyTo(long offset, byte[] buffer, int destOffset, int size) {
    throw new UnsupportedOperationException(
        String.format("Cannot copy from empty buffer for index: %s, segment: %s, table: %s", _segmentName,
            _tableNameWithType));
  }

  @Override
  public void readFrom(long offset, byte[] buffer, int srcOffset, int size) {
    throw new UnsupportedOperationException(
        String.format("Cannot write to empty buffer for index: %s, segment: %s, table: %s", _segmentName,
            _tableNameWithType));
  }

  @Override
  public void readFrom(long offset, ByteBuffer buffer) {
    throw new UnsupportedOperationException(
        String.format("Cannot write to empty buffer for index: %s, segment: %s, table: %s", _segmentName,
            _tableNameWithType));
  }

  @Override
  public void readFrom(long offset, java.io.File file, long srcOffset, long size)
      throws IOException {
    throw new UnsupportedOperationException(
        String.format("Cannot write to empty buffer for index: %s, segment: %s, table: %s", _segmentName,
            _tableNameWithType));
  }

  @Override
  public long size() {
    return 0; // Zero-size buffer
  }

  @Override
  public ByteOrder order() {
    return ByteOrder.BIG_ENDIAN; // Default to big-endian for consistency
  }

  @Override
  public PinotDataBuffer view(long start, long end, ByteOrder byteOrder) {
    if (start == 0 && end == 0) {
      return this; // Return self for zero-size view
    }
    throw new IllegalArgumentException(
        String.format("Invalid view range [%d, %d) for empty buffer. Index: %s, segment: %s, table: %s", start, end,
            _segmentName, _tableNameWithType));
  }

  @Override
  public void flush() {
    // No-op for empty buffer
  }

  @Override
  public void release()
      throws IOException {
    // No-op for empty buffer
  }

  @Override
  public ByteBuffer toDirectByteBuffer(long offset, int size, ByteOrder byteOrder) {
    if (size == 0) {
      return ByteBuffer.allocate(0).order(byteOrder);
    }
    throw new IllegalArgumentException(
        String.format("Cannot create ByteBuffer of size %d from empty buffer. Index: %s, segment: %s, table: %s", size,
            _segmentName, _tableNameWithType));
  }

  @Override
  public ImmutableRoaringBitmap viewAsRoaringBitmap(long offset, int length) {
    throw new IllegalArgumentException(
        String.format("Cannot create RoaringBitmap of length %d from empty buffer. Index: %s, segment: %s, table: %s",
            length, _segmentName, _tableNameWithType));
  }

  @Override
  public void appendAsByteBuffers(java.util.List<ByteBuffer> appendTo) {
    // No-op for empty buffer
  }


  /**
   * Gets the segment name for this empty buffer
   * @return The segment name
   */
  public String getSegmentName() {
    return _segmentName;
  }

  /**
   * Gets the table name with type for this empty buffer
   * @return The table name with type
   */
  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  @Override
  public String toString() {
    return String.format("EmptyIndexBuffer{ segmentName=%s, tableNameWithType=%s, segmentPath=%s, size=0}",
        _segmentName, _tableNameWithType);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof EmptyIndexBuffer)) {
      return false;
    }
    EmptyIndexBuffer other = (EmptyIndexBuffer) obj;
    return _segmentName.equals(
        other._segmentName) && _tableNameWithType.equals(other._tableNameWithType);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(_segmentName, _tableNameWithType);
  }
}
