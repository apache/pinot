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
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Covers [MapKeyIndexReader] over both shapes of underlying reader: one that implements the selective
/// [ForwardIndexReader#getMapValue] override (as the mutable forward index does), and one that only implements
/// [ForwardIndexReader#getMap] and therefore falls through to the default. Both must agree.
public class MapKeyIndexReaderTest {
  private static final Map<String, Object> MAP =
      Map.of("k8s.workload.name", "pinot-server", "k8s.workload.replicas", 3);

  @Test
  public void testSelectiveReader() {
    assertReaderBehavior(new SelectiveReader());
  }

  /// The immutable sparse-key path inherits the default `getMapValue`. It has to keep working unchanged.
  @Test
  public void testReaderWithoutSelectiveOverride() {
    assertReaderBehavior(new FullMapOnlyReader());
  }

  private static void assertReaderBehavior(ForwardIndexReader reader) {
    FieldSpec stringSpec = new DimensionFieldSpec("value", DataType.STRING, true);
    assertEquals(new MapKeyIndexReader(reader, "k8s.workload.name", stringSpec).getString(0, null), "pinot-server");

    // A key that is absent from the map resolves to the field spec's default null value, not to null.
    assertEquals(new MapKeyIndexReader(reader, "missing", stringSpec).getString(0, null),
        stringSpec.getDefaultNullValue());

    FieldSpec intSpec = new DimensionFieldSpec("value", DataType.INT, true);
    assertEquals(new MapKeyIndexReader(reader, "k8s.workload.replicas", intSpec).getInt(0, null), 3);
  }

  /// Mirrors the mutable forward index: answers a single key without materializing the map.
  private static class SelectiveReader extends BaseReader {
    @Override
    @Nullable
    public Object getMapValue(int docId, ForwardIndexReaderContext context, String key) {
      return MAP.get(key);
    }
  }

  /// Mirrors a reader that only knows how to hand back the whole map.
  private static class FullMapOnlyReader extends BaseReader {
  }

  @SuppressWarnings("rawtypes")
  private abstract static class BaseReader implements ForwardIndexReader {
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
