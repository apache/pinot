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
package org.apache.pinot.segment.local.segment.index.map;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.MapUtils.PreparedMapKey;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/// Covers [MapKeyIndexReader] over both shapes of underlying reader: one that implements the selective
/// [ForwardIndexReader#getMapEntryValue] override (as the mutable forward index does), and one that only implements
/// [ForwardIndexReader#getMap] and therefore falls through to the default. Both must agree.
public class MapKeyIndexReaderTest {
  private static final Map<String, Object> MAP =
      Map.of("k8s.workload.name", "pinot-server", "k8s.workload.replicas", 3,
          "longValue", 9999999999L, "doubleValue", 1.5d);

  @Test
  public void testSelectiveReader() {
    assertReaderBehavior(new SelectiveReader());
  }

  /// The immutable sparse-key path inherits the default `getMapEntryValue`. It has to keep working unchanged.
  @Test
  public void testReaderWithoutSelectiveOverride() {
    assertReaderBehavior(new FullMapOnlyReader());
  }

  @Test
  public void testStringOverloadDelegatesToSelectiveLookup() {
    assertEquals(new SelectiveReader().getMapEntryValue(0, null, "k8s.workload.name"), "pinot-server");
  }

  /// The numeric accessors fast-path only the exact boxed type Jackson yields for that JSON shape, and fall back to
  /// the string round trip otherwise. Pinning both halves stops a future broadening to `Number#intValue()` from
  /// silently truncating a decimal that currently throws.
  @Test
  public void testNumericAccessorsAcrossBoxedTypes() {
    ForwardIndexReader<ForwardIndexReaderContext> reader = new SelectiveReader();
    FieldSpec longSpec = new DimensionFieldSpec("value", DataType.LONG, true);
    FieldSpec intSpec = new DimensionFieldSpec("value", DataType.INT, true);
    FieldSpec doubleSpec = new DimensionFieldSpec("value", DataType.DOUBLE, true);
    FieldSpec floatSpec = new DimensionFieldSpec("value", DataType.FLOAT, true);

    // Long value through the Long branch, and an Integer widened through the Integer branch.
    assertEquals(new MapKeyIndexReader(reader, "longValue", longSpec).getLong(0, null), 9999999999L);
    assertEquals(new MapKeyIndexReader(reader, "k8s.workload.replicas", longSpec).getLong(0, null), 3L);
    assertEquals(new MapKeyIndexReader(reader, "doubleValue", doubleSpec).getDouble(0, null), 1.5d);
    // getFloat has no exact-type fast path; it still parses the rendered string.
    assertEquals(new MapKeyIndexReader(reader, "doubleValue", floatSpec).getFloat(0, null), 1.5f);

    // A decimal read as INT or LONG must keep failing rather than being silently truncated.
    assertThrows(NumberFormatException.class,
        () -> new MapKeyIndexReader(reader, "doubleValue", intSpec).getInt(0, null));
    assertThrows(NumberFormatException.class,
        () -> new MapKeyIndexReader(reader, "doubleValue", longSpec).getLong(0, null));
  }

  private static void assertReaderBehavior(ForwardIndexReader<ForwardIndexReaderContext> reader) {
    FieldSpec stringSpec = new DimensionFieldSpec("value", DataType.STRING, true);
    assertEquals(new MapKeyIndexReader(reader, "k8s.workload.name", stringSpec).getString(0, null), "pinot-server");

    // A key that is absent from the map resolves to the field spec's default null value, not to null.
    assertEquals(new MapKeyIndexReader(reader, "missing", stringSpec).getString(0, null),
        stringSpec.getDefaultNullValue());

    FieldSpec intSpec = new DimensionFieldSpec("value", DataType.INT, true);
    assertEquals(new MapKeyIndexReader(reader, "k8s.workload.replicas", intSpec).getInt(0, null), 3);

    // A STRING-valued MAP may still carry numeric entries; those must render as their canonical text.
    assertEquals(new MapKeyIndexReader(reader, "longValue", stringSpec).getString(0, null), "9999999999");
  }

  /// Mirrors the mutable forward index: answers a single key without materializing the map.
  private static class SelectiveReader extends BaseReader {
    @Nullable
    @Override
    public Object getMapEntryValue(int docId, ForwardIndexReaderContext context, PreparedMapKey key) {
      return MAP.get(key.getKey());
    }

    @Override
    public Map<String, Object> getMap(int docId, ForwardIndexReaderContext context) {
      throw new AssertionError("Selective lookup must not materialize the full map");
    }
  }

  /// Mirrors a reader that only knows how to hand back the whole map.
  private static class FullMapOnlyReader extends BaseReader {
  }

  private abstract static class BaseReader implements ForwardIndexReader<ForwardIndexReaderContext> {
    @Override
    public Map<String, Object> getMap(int docId, ForwardIndexReaderContext context) {
      return MAP;
    }

    @Override
    public boolean isDictionaryEncoded() {
      return false;
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Override
    public DataType getStoredType() {
      return DataType.MAP;
    }

    @Override
    public void close() {
    }
  }
}
