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
package org.apache.pinot.broker.transport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;

import static java.nio.charset.StandardCharsets.UTF_8;


/// Validates the complete V4 frame before invoking Pinot's ordinary decoder. Native responses have independent
/// bounds on sections, nested allocation lengths, rows, columns and referenced variable data. MAP, OBJECT and
/// UNKNOWN results are unsupported because their deferred codecs have their own allocation formats. This does
/// not change the legacy decoder or establish a heap bound for arbitrary query reduction or result formatting.
/// The input must remain immutable for the duration of this call; its position and limit are preserved.
public final class NativeDataTableDecoder {
  private static final int HEADER_BYTES = 13 * Integer.BYTES;
  private static final int MAX_PAYLOAD_BYTES = 16 * 1024 * 1024;
  private static final int MAX_ROWS = 1_000_000;
  private static final int MAX_COLUMNS = 1024;
  private static final int MAX_DICTIONARY_ENTRIES = 262_144;
  private static final int MAX_VALUE_BYTES = 1024 * 1024;
  private static final int MAX_METADATA_BYTES = 64 * 1024;
  private static final int MAX_DECIMAL_BYTES = 4096;
  private static final long MAX_REFERENCED_BYTES = 64L * 1024 * 1024;

  private NativeDataTableDecoder() {
  }

  public static DataTable decode(ByteBuffer payload) throws IOException {
    try {
      ByteBuffer input = payload.slice().order(ByteOrder.BIG_ENDIAN);
      require(input.remaining() >= HEADER_BYTES + Integer.BYTES && input.remaining() <= MAX_PAYLOAD_BYTES);
      require(input.getInt() == DataTableFactory.VERSION_4);
      int rows = bounded(input.getInt(), MAX_ROWS);
      int columns = bounded(input.getInt(), MAX_COLUMNS);
      ByteBuffer[] sections = new ByteBuffer[5];
      int next = HEADER_BYTES;
      for (int i = 0; i < sections.length; i++) {
        int offset = input.getInt();
        int length = input.getInt();
        require(offset == next && length >= 0 && length <= input.limit() - next);
        sections[i] = slice(input, offset, length);
        next += length;
      }
      ByteBuffer tail = slice(input, next, input.limit() - next);
      int metadataLength = bounded(tail.getInt(), MAX_METADATA_BYTES);
      require(metadataLength == tail.remaining());
      validateExceptions(sections[0]);
      int dictionarySize = validateDictionary(sections[1]);
      ColumnDataType[] types = validateSchema(sections[2], columns);
      validateMetadata(tail);
      validateValues(rows, types, dictionarySize, sections[3], sections[4]);
      return DataTableFactory.getDataTable(payload.slice().order(ByteOrder.BIG_ENDIAN));
    } catch (RuntimeException e) {
      throw new IOException("Invalid or unsupported native V4 DataTable", e);
    }
  }

  private static void validateExceptions(ByteBuffer input) throws IOException {
    if (!input.hasRemaining()) {
      return;
    }
    int count = count(input, 256, 8);
    Set<Integer> codes = new HashSet<>();
    for (int i = 0; i < count; i++) {
      require(codes.add(input.getInt()));
      skipString(input, MAX_METADATA_BYTES);
    }
    require(!input.hasRemaining());
  }

  private static int validateDictionary(ByteBuffer input) throws IOException {
    if (!input.hasRemaining()) {
      return 0;
    }
    int count = count(input, MAX_DICTIONARY_ENTRIES, Integer.BYTES);
    for (int i = 0; i < count; i++) {
      skipString(input, MAX_VALUE_BYTES);
    }
    require(!input.hasRemaining());
    return count;
  }

  private static ColumnDataType[] validateSchema(ByteBuffer input, int columns) throws IOException {
    if (!input.hasRemaining()) {
      require(columns == 0);
      return new ColumnDataType[0];
    }
    int count = count(input, MAX_COLUMNS, 2 * Integer.BYTES);
    require(count == columns);
    for (int i = 0; i < count; i++) {
      skipString(input, 16 * 1024);
    }
    ColumnDataType[] types = new ColumnDataType[count];
    for (int i = 0; i < count; i++) {
      int length = bounded(input.getInt(), 64);
      require(length <= input.remaining());
      byte[] name = new byte[length];
      input.get(name);
      ColumnDataType type = ColumnDataType.valueOf(new String(name, UTF_8)).getStoredType();
      require(type != ColumnDataType.OBJECT && type != ColumnDataType.UNKNOWN && type != ColumnDataType.MAP);
      types[i] = type;
    }
    require(!input.hasRemaining());
    return types;
  }

  private static void validateMetadata(ByteBuffer input) throws IOException {
    if (!input.hasRemaining()) {
      return;
    }
    int count = count(input, MetadataKey.values().length, Integer.BYTES + 1);
    Set<MetadataKey> keys = new HashSet<>();
    for (int i = 0; i < count; i++) {
      MetadataKey key = MetadataKey.getById(input.getInt());
      require(key != null && keys.add(key));
      switch (key.getValueType()) {
        case INT -> skip(input, Integer.BYTES);
        case LONG -> skip(input, Long.BYTES);
        case BOOLEAN -> {
          byte value = input.get();
          require(value == 0 || value == 1);
        }
        case STRING -> skipString(input, MAX_METADATA_BYTES);
        default -> throw new IOException("Unsupported native metadata value");
      }
    }
    require(!input.hasRemaining());
  }

  private static void validateValues(int rows, ColumnDataType[] types, int dictionarySize, ByteBuffer fixed,
      ByteBuffer variable) throws IOException {
    int rowBytes = 0;
    for (ColumnDataType type : types) {
      rowBytes += width(type);
    }
    long dataBytes = (long) rows * rowBytes;
    require((rows == 0 || rowBytes > 0) && dataBytes <= fixed.limit());
    long bitmapBytes = fixed.limit() - dataBytes;
    require(bitmapBytes == 0 || bitmapBytes == (long) types.length * 2 * Integer.BYTES);
    Budget budget = new Budget();
    for (int row = 0; row < rows; row++) {
      for (ColumnDataType type : types) {
        switch (type) {
          case INT, FLOAT -> skip(fixed, Integer.BYTES);
          case LONG, DOUBLE -> skip(fixed, Long.BYTES);
          case STRING -> dictionaryId(fixed.getInt(), dictionarySize);
          default -> validateVariable(type, variable, fixed.getInt(), fixed.getInt(), dictionarySize, budget);
        }
      }
    }
    if (bitmapBytes != 0) {
      for (int i = 0; i < types.length; i++) {
        int offset = fixed.getInt();
        int length = bounded(fixed.getInt(), 256 * 1024);
        ByteBuffer bitmap = slice(variable, offset, length);
        budget.add(length);
        if (length != 0) {
          validateBitmap(bitmap, rows, budget);
        }
      }
    }
    require(!fixed.hasRemaining());
  }

  private static void validateVariable(ColumnDataType type, ByteBuffer variable, int offset, int length,
      int dictionarySize, Budget budget) throws IOException {
    bounded(length, MAX_VALUE_BYTES);
    int elementBytes = switch (type) {
      case INT_ARRAY, FLOAT_ARRAY, STRING_ARRAY -> Integer.BYTES;
      case LONG_ARRAY, DOUBLE_ARRAY -> Long.BYTES;
      default -> 1;
    };
    if (type == ColumnDataType.BYTES || type == ColumnDataType.BIG_DECIMAL) {
      ByteBuffer value = slice(variable, offset, length);
      budget.add(length);
      if (type == ColumnDataType.BIG_DECIMAL) {
        validateDecimal(value.remaining());
      }
      return;
    }
    require(type == ColumnDataType.INT_ARRAY || type == ColumnDataType.LONG_ARRAY
        || type == ColumnDataType.FLOAT_ARRAY || type == ColumnDataType.DOUBLE_ARRAY
        || type == ColumnDataType.STRING_ARRAY || type == ColumnDataType.BYTES_ARRAY
        || type == ColumnDataType.BIG_DECIMAL_ARRAY);
    bounded(length, MAX_ROWS);
    budget.add((long) length * 16);
    if (type == ColumnDataType.BYTES_ARRAY || type == ColumnDataType.BIG_DECIMAL_ARRAY) {
      ByteBuffer values = slice(variable, offset, variable.limit() - offset);
      require(length <= values.remaining() / Integer.BYTES);
      for (int i = 0; i < length; i++) {
        int size = bounded(values.getInt(), MAX_VALUE_BYTES);
        if (type == ColumnDataType.BIG_DECIMAL_ARRAY) {
          validateDecimal(size);
        }
        skip(values, size);
        budget.add(size);
      }
    } else {
      ByteBuffer values = slice(variable, offset, Math.toIntExact((long) length * elementBytes));
      if (type == ColumnDataType.STRING_ARRAY) {
        for (int i = 0; i < length; i++) {
          dictionaryId(values.getInt(), dictionarySize);
        }
      }
    }
  }

  /// Check portable Roaring containers before the ordinary DataTable null-bitmap reader can allocate from them.
  private static void validateBitmap(ByteBuffer input, int rows, Budget budget) throws IOException {
    input.order(ByteOrder.LITTLE_ENDIAN);
    int cookie = input.getInt();
    boolean runs = (cookie & 0xffff) == 12347;
    require(runs || cookie == 12346);
    int containers = runs ? (cookie >>> 16) + 1 : input.getInt();
    bounded(containers, (rows + 65535) / 65536);
    byte[] runFlags = new byte[runs ? (containers + 7) / 8 : 0];
    input.get(runFlags);
    require(containers <= input.remaining() / 4);
    int[] keys = new int[containers];
    int[] cardinalities = new int[containers];
    for (int i = 0; i < containers; i++) {
      keys[i] = Short.toUnsignedInt(input.getShort());
      cardinalities[i] = Short.toUnsignedInt(input.getShort()) + 1;
      require((i == 0 || keys[i] > keys[i - 1]) && ((long) keys[i] << 16) < rows);
      budget.add((long) cardinalities[i] * Integer.BYTES);
    }
    int[] offsets = new int[containers];
    boolean offsetsPresent = !runs || containers >= 4;
    if (offsetsPresent) {
      for (int i = 0; i < containers; i++) {
        offsets[i] = input.getInt();
      }
    }
    for (int i = 0; i < containers; i++) {
      require(!offsetsPresent || offsets[i] == input.position());
      int actualCardinality = 0;
      int maximum = -1;
      if (runs && (runFlags[i / 8] & (1 << (i % 8))) != 0) {
        int count = Short.toUnsignedInt(input.getShort());
        require(count <= input.remaining() / 4);
        for (int j = 0; j < count; j++) {
          int start = Short.toUnsignedInt(input.getShort());
          int length = Short.toUnsignedInt(input.getShort());
          require(start > maximum && start + length <= 65535);
          maximum = start + length;
          actualCardinality += length + 1;
        }
      } else if (cardinalities[i] > 4096) {
        require(input.remaining() >= 8192);
        for (int j = 0; j < 1024; j++) {
          long word = input.getLong();
          actualCardinality += Long.bitCount(word);
          if (word != 0) {
            maximum = j * 64 + 63 - Long.numberOfLeadingZeros(word);
          }
        }
      } else {
        require(cardinalities[i] <= input.remaining() / Short.BYTES);
        for (int j = 0; j < cardinalities[i]; j++) {
          int value = Short.toUnsignedInt(input.getShort());
          require(value > maximum);
          maximum = value;
        }
        actualCardinality = cardinalities[i];
      }
      require(actualCardinality == cardinalities[i] && (((long) keys[i] << 16) + maximum) < rows);
    }
    require(!input.hasRemaining());
  }

  private static void validateDecimal(int length) throws IOException {
    require(length >= 3 && length <= MAX_DECIMAL_BYTES);
  }

  private static int width(ColumnDataType type) {
    return type == ColumnDataType.INT || type == ColumnDataType.FLOAT || type == ColumnDataType.STRING ? 4 : 8;
  }

  private static void dictionaryId(int id, int size) throws IOException {
    require(id >= 0 && id < size);
  }

  private static int count(ByteBuffer input, int maximum, int minimumEntryBytes) throws IOException {
    int count = bounded(input.getInt(), maximum);
    require(count <= input.remaining() / minimumEntryBytes);
    return count;
  }

  private static void skipString(ByteBuffer input, int maximum) throws IOException {
    skip(input, bounded(input.getInt(), maximum));
  }

  private static void skip(ByteBuffer input, int length) throws IOException {
    require(length >= 0 && length <= input.remaining());
    input.position(input.position() + length);
  }

  private static ByteBuffer slice(ByteBuffer input, int offset, int length) throws IOException {
    require(offset >= 0 && offset <= input.limit() && length >= 0 && length <= input.limit() - offset);
    return input.slice(offset, length).order(ByteOrder.BIG_ENDIAN);
  }

  private static int bounded(int value, int maximum) throws IOException {
    require(value >= 0 && value <= maximum);
    return value;
  }

  private static void require(boolean valid) throws IOException {
    if (!valid) {
      throw new IOException("Invalid or unsupported native V4 DataTable");
    }
  }

  private static final class Budget {
    private long _bytes;

    private void add(long bytes) throws IOException {
      require(bytes >= 0 && bytes <= MAX_REFERENCED_BYTES - _bytes);
      _bytes += bytes;
    }
  }
}
