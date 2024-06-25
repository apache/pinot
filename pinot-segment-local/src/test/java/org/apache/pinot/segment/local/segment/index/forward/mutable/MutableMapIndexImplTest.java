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
package org.apache.pinot.segment.local.segment.index.forward.mutable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.map.MutableMapIndexImpl;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.config.table.MapIndexConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MutableMapIndexImplTest {
  private PinotDataBufferMemoryManager _memoryManager;

  @BeforeClass
  public void setUp() {
    _memoryManager = new DirectMemoryManager(MutableMapIndexImplTest.class.getName());
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _memoryManager.close();
  }

  @Test
  public void testAddOneKeyIntegerValue()
      throws IOException {
    MapIndexConfig config =
        new MapIndexConfig(false, 100, List.of(
            new DimensionFieldSpec("a", FieldSpec.DataType.INT, true)), false);

    MutableMapIndexImpl mdc = new MutableMapIndexImpl(config, _memoryManager, 1000, false,
        false, null, "test-segment");
    HashMap<String, Object> data = new HashMap<>();
    data.put("a", 1);
    mdc.add(data, 0);

    data = new HashMap<>();
    data.put("a", 2);
    mdc.add(data, 1);

    ForwardIndexReader reader = (ForwardIndexReader) mdc.getKeyReader("a", StandardIndexes.forward());
    ForwardIndexReaderContext ctx = reader.createContext();
    int result = reader.getInt(0, ctx);
    Assert.assertEquals(result, 1);

    result = reader.getInt(1, ctx);
    Assert.assertEquals(result, 2);
  }

  @Test
  public void testAddOneKeyIntegerValueDynamicKeys()
      throws IOException {
    MapIndexConfig config =
        new MapIndexConfig(false, 100, List.of(
            new DimensionFieldSpec("a", FieldSpec.DataType.INT, true)), true);

    MutableMapIndexImpl mdc = new MutableMapIndexImpl(config, _memoryManager, 1000, false,
        false, null, "test-segment");
    HashMap<String, Object> data = new HashMap<>();
    data.put("a", 1);
    mdc.add(data, 0);

    data = new HashMap<>();
    data.put("a", 2);
    mdc.add(data, 1);

    data = new HashMap<>();
    data.put("a", 5);
    data.put("b", "hello");
    mdc.add(data, 2);

    {
      ForwardIndexReader reader = (ForwardIndexReader) mdc.getKeyReader("a", StandardIndexes.forward());
      ForwardIndexReaderContext ctx = reader.createContext();
      int result = reader.getInt(0, ctx);
      Assert.assertEquals(result, 1);

      result = reader.getInt(1, ctx);
      Assert.assertEquals(result, 2);
    }
    {
      ForwardIndexReader reader = (ForwardIndexReader) mdc.getKeyReader("b", StandardIndexes.forward());
      ForwardIndexReaderContext ctx = reader.createContext();
      String result = reader.getString(2, ctx);
      Assert.assertEquals(result, "hello");

      result = reader.getString(0, ctx);
      Assert.assertEquals(result, "null");
      result = reader.getString(1, ctx);
      Assert.assertEquals(result, "null");
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testNumberOfPredefinedKeysExceedsMaxKeys() {
    MapIndexConfig config =
        new MapIndexConfig(false, 2,
            List.of(
                new DimensionFieldSpec("a", FieldSpec.DataType.INT, true),
                new DimensionFieldSpec("b", FieldSpec.DataType.INT, true),
                new DimensionFieldSpec("c", FieldSpec.DataType.INT, true)
            ),
            true);

    MutableMapIndexImpl mdc = new MutableMapIndexImpl(config, _memoryManager, 1000, false,
        false, null, "test-segment");
  }

  @Test
  public void testAddOneKeyIntegerValueDynamicKeysKeyLimit()
      throws IOException {
    MapIndexConfig config =
        new MapIndexConfig(false, 2, List.of(
            new DimensionFieldSpec("a", FieldSpec.DataType.INT, true)), true);

    MutableMapIndexImpl mdc = new MutableMapIndexImpl(config, _memoryManager, 1000, false,
        false, null, "test-segment");
    HashMap<String, Object> data = new HashMap<>();
    data.put("a", 1);
    mdc.add(data, 0);

    data = new HashMap<>();
    data.put("a", 2);
    mdc.add(data, 1);

    data = new HashMap<>();
    data.put("a", 5);
    data.put("b", "hello");
    mdc.add(data, 2);

    data = new HashMap<>();
    data.put("a", 5);
    data.put("b", "world");
    data.put("c", 2.0D);
    mdc.add(data, 3);

    {
      ForwardIndexReader reader = (ForwardIndexReader) mdc.getKeyReader("a", StandardIndexes.forward());
      ForwardIndexReaderContext ctx = reader.createContext();
      int result = reader.getInt(0, ctx);
      Assert.assertEquals(result, 1);

      result = reader.getInt(1, ctx);
      Assert.assertEquals(result, 2);
    }
    {
      ForwardIndexReader reader = (ForwardIndexReader) mdc.getKeyReader("b", StandardIndexes.forward());
      ForwardIndexReaderContext ctx = reader.createContext();
      String result = reader.getString(2, ctx);
      Assert.assertEquals(result, "hello");
      result = reader.getString(3, ctx);
      Assert.assertEquals(result, "world");

      result = reader.getString(0, ctx);
      Assert.assertEquals(result, "null");
      result = reader.getString(1, ctx);
      Assert.assertEquals(result, "null");
    }
    {
      ForwardIndexReader reader = (ForwardIndexReader) mdc.getKeyReader("c", StandardIndexes.forward());
      Assert.assertNull(reader);
    }
  }

  @Test
  public void testAddOneKeyStringValue()
      throws IOException {
    MapIndexConfig config =
        new MapIndexConfig(false, 100, List.of(
            new DimensionFieldSpec("a", FieldSpec.DataType.STRING, true)),
            false);
    MutableMapIndexImpl mdc = new MutableMapIndexImpl(config, _memoryManager, 1000, false,
        false, null, "test-segment");
    HashMap<String, Object> data = new HashMap<>();
    data.put("a", "hello");
    mdc.add(data, 0);

    data = new HashMap<>();
    data.put("a", "world");
    mdc.add(data, 1);

    ForwardIndexReader reader = (ForwardIndexReader) mdc.getKeyReader("a", StandardIndexes.forward());
    ForwardIndexReaderContext ctx = reader.createContext();
    String result = reader.getString(0, ctx);
    Assert.assertEquals(result, "hello");

    result = reader.getString(1, ctx);
    Assert.assertEquals(result, "world");
  }

  @Test
  public void testAddOneKeyDoubleValue()
      throws IOException {
    MapIndexConfig config =
        new MapIndexConfig(false, 100, List.of(
            new DimensionFieldSpec("a", FieldSpec.DataType.DOUBLE, true)),
            false);
    MutableMapIndexImpl mdc = new MutableMapIndexImpl(config, _memoryManager, 1000, false,
        false, null, "test-segment");
    HashMap<String, Object> data = new HashMap<>();
    double value1 = 2.0;
    data.put("a", value1);
    mdc.add(data, 0);

    data = new HashMap<>();
    double value2 = 3.0;
    data.put("a", value2);
    mdc.add(data, 1);

    ForwardIndexReader reader = (ForwardIndexReader) mdc.getKeyReader("a", StandardIndexes.forward());
    ForwardIndexReaderContext ctx = reader.createContext();
    double result = reader.getDouble(0, ctx);
    Assert.assertEquals(result, value1);

    result = reader.getDouble(1, ctx);
    Assert.assertEquals(result, value2);
  }

  @Test
  public void testAddTwoKeyIntegerValue()
      throws IOException {
    MapIndexConfig config =
        new MapIndexConfig(false, 100, List.of(
            new DimensionFieldSpec("a", FieldSpec.DataType.INT, true),
            new DimensionFieldSpec("b", FieldSpec.DataType.INT, true)
            ),
            false);
    MutableMapIndexImpl mdc = new MutableMapIndexImpl(config, _memoryManager, 1000, false,
        false, null, "test-segment");
    HashMap<String, Object> data = new HashMap<>();
    data.put("a", 1);
    mdc.add(data, 0);

    data = new HashMap<>();
    data.put("b", 2);
    mdc.add(data, 1);

    ForwardIndexReader aReader = (ForwardIndexReader) mdc.getKeyReader("a", StandardIndexes.forward());
    ForwardIndexReaderContext ctx = aReader.createContext();
    int result = aReader.getInt(0, ctx);
    Assert.assertEquals(result, 1);

    ForwardIndexReader bReader = (ForwardIndexReader) mdc.getKeyReader("b", StandardIndexes.forward());
    result = bReader.getInt(1, ctx);
    Assert.assertEquals(result, 2);
  }

  @Test
  public void testExceedMaxKeys()
      throws IOException {
    MapIndexConfig config =
        new MapIndexConfig(false, 100, List.of(
            new DimensionFieldSpec("a", FieldSpec.DataType.INT, true),
            new DimensionFieldSpec("b", FieldSpec.DataType.INT, true),
            new DimensionFieldSpec("c", FieldSpec.DataType.INT, true),
            new DimensionFieldSpec("d", FieldSpec.DataType.INT, true),
            new DimensionFieldSpec("e", FieldSpec.DataType.INT, true)
        ),
            false);
    MutableMapIndexImpl mdc = new MutableMapIndexImpl(config, _memoryManager, 1000, false,
        false, null, "test-segment");
    HashMap<String, Object> data = new HashMap<>();
    data.put("a", 1);
    data.put("b", 1);
    data.put("c", 1);
    data.put("d", 1);
    data.put("e", 1);
    mdc.add(data, 0);
    Assert.assertNotNull(mdc.getKeyIndexes("a"));
    Assert.assertNotNull(mdc.getKeyIndexes("b"));
    Assert.assertNotNull(mdc.getKeyIndexes("c"));
    Assert.assertNotNull(mdc.getKeyIndexes("d"));
    Assert.assertNotNull(mdc.getKeyIndexes("e"));

    data = new HashMap<>();
    data.put("f", 2);
    mdc.add(data, 1);
    Assert.assertNull(mdc.getKeyIndexes("f"));
  }
}
