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
package org.apache.pinot.segment.local.segment.creator.impl.map;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;

public class MapIndexHeader {
  final static int VERSION = 1;
  private final List<DenseMapHeader> _mapIndexes;

  public MapIndexHeader() {
    _mapIndexes = new LinkedList<>();
  }

  MapIndexHeader(List<DenseMapHeader> indexes) {
    _mapIndexes = indexes;
  }

  public void addMapIndex(DenseMapHeader mapIndex) {
    _mapIndexes.add(mapIndex);
  }

  public DenseMapHeader getMapIndex() {
    return _mapIndexes.get(0);
  }

  public long write(PinotDataBuffer buffer, long offset) throws IOException {
    PinotDataBufferWriter writer = new PinotDataBufferWriter(buffer);
    return write(writer, offset);
  }

  public long size() throws IOException {
    HeaderSizeComputer size = new HeaderSizeComputer();

    write(size, 0);

    return size.size();
  }

  public long write(MapHeaderWriter writer, long offset) throws IOException {
    // Write the header version
    offset = writer.putInt(offset, VERSION);

    // Write the number of map indexes
    offset = writer.putInt(offset, _mapIndexes.size());

    // Iterate over each map index and write it
    for (DenseMapHeader mapMeta : _mapIndexes) {
      offset = mapMeta.write(writer, offset);
    }

    return offset;
  }

  public static Pair<MapIndexHeader, Integer> read(PinotDataBuffer buffer, int offset)
    throws IOException {
    // read the version
    int version = buffer.getInt(offset);
    offset += Integer.BYTES;
    assert version == 1;

    // read the number of indexes
    int numIndexes = buffer.getInt(offset);
    offset += Integer.BYTES;

    // Iterate over the indexes and create the appropriate index
    List<DenseMapHeader> indexes = new LinkedList<>();
    for (int i = 0; i < numIndexes; i++) {
      Pair<DenseMapHeader, Integer> result = DenseMapHeader.read(buffer, offset);
      offset = result.getRight();
      indexes.add(result.getLeft());
    }

    return new ImmutablePair<>(new MapIndexHeader(indexes), offset);
  }

  public static Pair<String, Integer> readString(PinotDataBuffer buffer, final int stringOffset) {
    int offset = stringOffset;
    // Read the length of the string
    final int len = buffer.getInt(offset);
    offset += Integer.BYTES;

    // Read the string
    char[] chars = new char[len];
    for (int i = 0; i < len; i++) {
      char ch = buffer.getChar(offset);
      offset += Character.BYTES;
      chars[i] = ch;
    }

    return new ImmutablePair<>(new String(chars), offset);
  }

  public static Pair<Comparable<?>, Integer> readValue(PinotDataBuffer buffer,
      final int valueOffset, FieldSpec.DataType type) {
    Comparable<?> value = null;
    int offset = valueOffset;
    switch (type) {
      case INT:
        value = buffer.getInt(offset);
        offset += Integer.BYTES;
        break;
      case LONG:
        value = buffer.getLong(offset);
        offset += Long.BYTES;
        break;
      case FLOAT:
        value = buffer.getFloat(offset);
        offset += Float.BYTES;
        break;
      case DOUBLE:
        value = buffer.getDouble(offset);
        offset += Double.BYTES;
        break;
      case BOOLEAN:
        value = buffer.getByte(offset) == 1;
        offset += Byte.BYTES;
        break;
      case STRING:
        Pair<String, Integer> result = readString(buffer, offset);
        value = result.getLeft();
        offset = result.getRight();
        break;
      case BIG_DECIMAL:
      case TIMESTAMP:
      case JSON:
      case BYTES:
      case STRUCT:
      case MAP:
      case LIST:
      case UNKNOWN:
      default:
        throw new UnsupportedOperationException();
    }

    return new ImmutablePair<>(value, offset);
  }

  public boolean equals(Object obj) {
    if (obj instanceof MapIndexHeader) {
      return _mapIndexes.equals(((MapIndexHeader) obj)._mapIndexes);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(_mapIndexes);
  }

  public interface MapHeaderWriter {
    long putInt(long offset, int value);
    long putLong(long offset, long value);
    long putString(long offset, String value);
    long putByte(long offset, byte value);
    long putFloat(long offset, float value);
    long putDouble(long offset, double value);
    default long putValue(long offset, FieldSpec.DataType type, Comparable<?> value) {
      switch (type) {
        case INT:
          offset = putInt(offset, ((Integer) value));
          break;
        case LONG:
          offset = putLong(offset, ((Long) value));
          break;
        case FLOAT:
          offset = putFloat(offset, ((Float) value));
          break;
        case DOUBLE:
          offset = putDouble(offset, ((Double) value));
          break;
        case STRING:
          offset = putString(offset, (String) value);
          break;
        case BOOLEAN:
          offset = putByte(offset, (byte) (((Boolean) value) ? 1 : 0));
          break;
        case BIG_DECIMAL:
        case TIMESTAMP:
        case JSON:
        case BYTES:
        case STRUCT:
        case MAP:
        case LIST:
        case UNKNOWN:
        default:
          throw new UnsupportedOperationException();
      }

      return offset;
    }
  }

  public static class PinotDataBufferWriter implements MapHeaderWriter {
    private final PinotDataBuffer _buffer;

    public PinotDataBufferWriter(PinotDataBuffer buffer) {
      _buffer = buffer;
    }

    @Override
    public long putInt(long offset, int value) {
      _buffer.putInt(offset, value);
      return offset + Integer.BYTES;
    }

    @Override
    public long putLong(long offset, long value) {
      _buffer.putLong(offset, value);
      return offset + Long.BYTES;
    }

    @Override
    public long putString(long offset, String value) {
      char[] chars = value.toCharArray();
      _buffer.putInt(offset, chars.length);
      offset += Integer.BYTES;

      for (char aChar : chars) {
        _buffer.putChar(offset, aChar);
        offset += Character.BYTES;
      }

      return offset;
    }

    @Override
    public long putByte(long offset, byte value) {
      _buffer.putByte(offset, value);
      return offset + Byte.BYTES;
    }

    @Override
    public long putFloat(long offset, float value) {
      _buffer.putFloat(offset, value);
      return offset + Float.BYTES;
    }

    @Override
    public long putDouble(long offset, double value) {
      _buffer.putDouble(offset, value);
      return offset + Double.BYTES;
    }
  }

  public static class HeaderSizeComputer implements MapHeaderWriter {
    private long _size = 0;

    public long size() {
      return _size;
    }

    @Override
    public long putInt(long offset, int value) {
      _size += Integer.BYTES;
      return offset + Integer.BYTES;
    }

    @Override
    public long putLong(long offset, long value) {
      _size += Long.BYTES;
      return offset + Long.BYTES;
    }

    @Override
    public long putString(long offset, String value) {
      _size += Integer.BYTES;
      _size += value.length() * Character.BYTES;
      return offset + Integer.BYTES + value.length() * Character.BYTES;
    }

    @Override
    public long putByte(long offset, byte value) {
      _size += Byte.BYTES;
      return offset + Byte.BYTES;
    }

    @Override
    public long putFloat(long offset, float value) {
      _size += Float.BYTES;
      return offset + Float.BYTES;
    }

    @Override
    public long putDouble(long offset, double value) {
      _size += Double.BYTES;
      return offset + Double.BYTES;
    }
  }
}
