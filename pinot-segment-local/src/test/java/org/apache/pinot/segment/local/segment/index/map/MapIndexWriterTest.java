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

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.segment.local.segment.creator.impl.map.DenseMapHeader;
import org.apache.pinot.segment.local.segment.creator.impl.map.MapIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.map.MapIndexHeader;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexReaderFactory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.MapColumnStatistics;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.config.table.MapIndexConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.spi.V1Constants.Indexes.MAP_FORWARD_INDEX_FILE_EXTENSION;

public class MapIndexWriterTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "MapIndexTest");
  private static final String MAP_COLUMN_NAME = "test_map";

  @BeforeMethod
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @Test
  public void testingWritingMultipleChunks() {
    List<DimensionFieldSpec> keys = List.of(
        new DimensionFieldSpec("a", FieldSpec.DataType.INT, true),
        new DimensionFieldSpec("b", FieldSpec.DataType.INT, true)
    );
    List<HashMap<String, Object>> records = createTestData(keys, 2000);
    MapIndexConfig config = new MapIndexConfig();
    config.setDenseKeys(keys);
    config.setMaxKeys(4);
    try {
      String mapIndexFilePath = createIndex(config, records);

      File mapIndexFile = new File(mapIndexFilePath);
      long size = Files.size(mapIndexFile.toPath());
      try (PinotDataBuffer keyBuffer = PinotDataBuffer.mapFile(mapIndexFile, true, 0, size, ByteOrder.BIG_ENDIAN,
          "test")) {
        Pair<MapIndexHeader, Integer> result = MapIndexHeader.read(keyBuffer, 0);
        MapIndexHeader header = result.getLeft();

        Assert.assertEquals(header.getMapIndex().getKeys().size(), 2);
        for (DenseMapHeader.DenseKeyMetadata keyMeta : header.getMapIndex().getKeys()) {
          String key = keyMeta.getName();
          if (keyMeta.getColumnMetadata().getColumnName().equals("a")) {
            ColumnMetadata aMd = keyMeta.getColumnMetadata();
            Assert.assertEquals(keyMeta.getIndexOffset(StandardIndexes.forward()), 326);
            Assert.assertEquals(aMd.getFieldSpec().getDataType(), FieldSpec.DataType.INT);
            Assert.assertEquals(aMd.getColumnName(), "a");

            long offset = keyMeta.getIndexOffset(StandardIndexes.forward());
            long keyIndexSize = keyMeta.getColumnMetadata().getIndexSizeMap().get(StandardIndexes.forward());
            PinotDataBuffer innerBuffer =
                PinotDataBuffer.mapFile(mapIndexFile, false, offset, keyIndexSize, ByteOrder.BIG_ENDIAN, "test_a");
            ForwardIndexReader<ForwardIndexReaderContext> innerFwdReader =
                ForwardIndexReaderFactory.createIndexReader(innerBuffer, keyMeta.getColumnMetadata());

            testForwardIndexReader("a", innerFwdReader, records);
          } else if (keyMeta.getColumnMetadata().getColumnName().equals("b")) {
            Assert.assertEquals(keyMeta.getIndexOffset(StandardIndexes.forward()), 8390);
          } else {
            Assert.fail();
          }
        }
      } catch (Exception ex) {
        throw ex;
      }
    } catch (Exception ex) {
      Assert.fail("Error Creating Index", ex);
    }
  }

  @Test
  public void testingWritingDropKeyIfNotInDenseSet() {
    List<DimensionFieldSpec> keys = List.of(
        new DimensionFieldSpec("a", FieldSpec.DataType.INT, true),
        new DimensionFieldSpec("b", FieldSpec.DataType.INT, true)
    );

    // Create a set of data that has two extra keys that are not in the config
    List<HashMap<String, Object>> records = createTestData(
        List.of(
            new DimensionFieldSpec("a", FieldSpec.DataType.INT, true),
            new DimensionFieldSpec("b", FieldSpec.DataType.INT, true),
            new DimensionFieldSpec("c", FieldSpec.DataType.INT, true),
            new DimensionFieldSpec("d", FieldSpec.DataType.INT, true)
        ),
        2000);

    MapIndexConfig config = new MapIndexConfig();
    config.setDynamicallyCreateDenseKeys(false);
    config.setDenseKeys(keys);
    config.setMaxKeys(4);

    try {
      String mapIndexFilePath = createIndex(config, records);

      File mapIndexFile = new File(mapIndexFilePath);
      long size = Files.size(mapIndexFile.toPath());
      try (PinotDataBuffer keyBuffer = PinotDataBuffer.mapFile(mapIndexFile, true, 0, size, ByteOrder.BIG_ENDIAN,
          "test")) {
        Pair<MapIndexHeader, Integer> result = MapIndexHeader.read(keyBuffer, 0);
        MapIndexHeader header = result.getLeft();
        Assert.assertEquals(header.getMapIndex().getKeys().size(), 2);

        Assert.assertNotNull(header.getMapIndex().getKey("a"), "Header does not contain key 'a'");
        Assert.assertNotNull(header.getMapIndex().getKey("b"), "Header does not contain key 'a'");
        Assert.assertNull(header.getMapIndex().getKey("c"), "Header should not contain key 'a'");
        Assert.assertNull(header.getMapIndex().getKey("d"), "Header should not contain key 'a'");

        for (DenseMapHeader.DenseKeyMetadata keyMeta : header.getMapIndex().getKeys()) {
          if (keyMeta.getColumnMetadata().getColumnName().equals("a")) {
            ColumnMetadata aMd = keyMeta.getColumnMetadata();
            Assert.assertEquals(keyMeta.getIndexOffset(StandardIndexes.forward()), 326);
            Assert.assertEquals(aMd.getFieldSpec().getDataType(), FieldSpec.DataType.INT);
            Assert.assertEquals(aMd.getColumnName(), "a");

            long offset = keyMeta.getIndexOffset(StandardIndexes.forward());
            long keyIndexSize = keyMeta.getColumnMetadata().getIndexSizeMap().get(StandardIndexes.forward());
            PinotDataBuffer innerBuffer =
                PinotDataBuffer.mapFile(mapIndexFile, false, offset, keyIndexSize, ByteOrder.BIG_ENDIAN, "test_a");
            ForwardIndexReader<ForwardIndexReaderContext> innerFwdReader =
                ForwardIndexReaderFactory.createIndexReader(innerBuffer, keyMeta.getColumnMetadata());

            testForwardIndexReader("a", innerFwdReader, records);
          } else if (keyMeta.getColumnMetadata().getColumnName().equals("b")) {
            Assert.assertEquals(keyMeta.getIndexOffset(StandardIndexes.forward()), 8390);
          } else {
            Assert.fail();
          }
        }
      } catch (Exception ex) {
        throw ex;
      }
    } catch (Exception ex) {
      Assert.fail("Error Creating Index", ex);
    }
  }

  @Test
  public void testingDynamicallyCreateDenseKey() {
    List<DimensionFieldSpec> keys = List.of(
        new DimensionFieldSpec("a", FieldSpec.DataType.INT, true),
        new DimensionFieldSpec("b", FieldSpec.DataType.INT, true)
    );

    // Create a set of data that has two extra keys that are not in the config
    // This will cause the key "c" to be dynamically created at teh start of teh segment
    List<HashMap<String, Object>> records = createTestData(
        List.of(
            new DimensionFieldSpec("a", FieldSpec.DataType.INT, true),
            new DimensionFieldSpec("b", FieldSpec.DataType.INT, true)
        ),
        2000);

    // This will test creating a key after the index has already been written to
    records.get(15).put("c", 1500);
    records.get(16).put("c", 1600);
    records.get(17).put("c", 1700);
    records.get(15).put("d", 1500);
    records.get(16).put("d", 1600);
    records.get(17).put("d", 1700);

    MapIndexConfig config = new MapIndexConfig();
    config.setDynamicallyCreateDenseKeys(true);
    config.setDenseKeys(keys);
    config.setMaxKeys(4);

    try {
      String mapIndexFilePath = createIndex(config, records);

      File mapIndexFile = new File(mapIndexFilePath);
      long size = Files.size(mapIndexFile.toPath());
      try (PinotDataBuffer keyBuffer = PinotDataBuffer.mapFile(mapIndexFile, true, 0, size, ByteOrder.BIG_ENDIAN,
          "test")) {
        Pair<MapIndexHeader, Integer> result = MapIndexHeader.read(keyBuffer, 0);
        MapIndexHeader header = result.getLeft();
        Assert.assertEquals(header.getMapIndex().getKeys().size(), 4);

        Assert.assertNotNull(header.getMapIndex().getKey("a"), "Header does not contain key 'a'");
        Assert.assertNotNull(header.getMapIndex().getKey("b"), "Header does not contain key 'b'");
        Assert.assertNotNull(header.getMapIndex().getKey("c"), "Header does not contain key 'c'");
        Assert.assertNotNull(header.getMapIndex().getKey("d"), "Header does not contain key 'd'");

        for (DenseMapHeader.DenseKeyMetadata keyMeta : header.getMapIndex().getKeys()) {
          if (keyMeta.getColumnMetadata().getColumnName().equals("a")) {
            ColumnMetadata aMd = keyMeta.getColumnMetadata();
            Assert.assertEquals(aMd.getFieldSpec().getDataType(), FieldSpec.DataType.INT);
            Assert.assertEquals(aMd.getColumnName(), "a");

            long offset = keyMeta.getIndexOffset(StandardIndexes.forward());
            long keyIndexSize = keyMeta.getColumnMetadata().getIndexSizeMap().get(StandardIndexes.forward());
            PinotDataBuffer innerBuffer =
                PinotDataBuffer.mapFile(mapIndexFile, false, offset, keyIndexSize, ByteOrder.BIG_ENDIAN, "test_a");
            ForwardIndexReader<ForwardIndexReaderContext> innerFwdReader =
                ForwardIndexReaderFactory.createIndexReader(innerBuffer, keyMeta.getColumnMetadata());

            testForwardIndexReader("a", innerFwdReader, records);
          } else if (keyMeta.getColumnMetadata().getColumnName().equals("b")) {
            ColumnMetadata aMd = keyMeta.getColumnMetadata();
            Assert.assertEquals(aMd.getFieldSpec().getDataType(), FieldSpec.DataType.INT);
            Assert.assertEquals(aMd.getColumnName(), "b");

            long offset = keyMeta.getIndexOffset(StandardIndexes.forward());
            long keyIndexSize = keyMeta.getColumnMetadata().getIndexSizeMap().get(StandardIndexes.forward());
            PinotDataBuffer innerBuffer =
                PinotDataBuffer.mapFile(mapIndexFile, false, offset, keyIndexSize, ByteOrder.BIG_ENDIAN, "test_b");
            ForwardIndexReader<ForwardIndexReaderContext> innerFwdReader =
                ForwardIndexReaderFactory.createIndexReader(innerBuffer, keyMeta.getColumnMetadata());

            testForwardIndexReader("b", innerFwdReader, records);
          } else if (keyMeta.getColumnMetadata().getColumnName().equals("c")) {
          } else if (keyMeta.getColumnMetadata().getColumnName().equals("d")) {
            ColumnMetadata aMd = keyMeta.getColumnMetadata();
            Assert.assertEquals(aMd.getFieldSpec().getDataType(), FieldSpec.DataType.INT);
            Assert.assertEquals(aMd.getColumnName(), "d");

            long offset = keyMeta.getIndexOffset(StandardIndexes.forward());
            long keyIndexSize = keyMeta.getColumnMetadata().getIndexSizeMap().get(StandardIndexes.forward());
            PinotDataBuffer innerBuffer =
                PinotDataBuffer.mapFile(mapIndexFile, false, offset, keyIndexSize, ByteOrder.BIG_ENDIAN, "test_d");
            ForwardIndexReader<ForwardIndexReaderContext> innerFwdReader =
                ForwardIndexReaderFactory.createIndexReader(innerBuffer, keyMeta.getColumnMetadata());

            ForwardIndexReaderContext context = innerFwdReader.createContext();
            for (int i = 0; i < 15; i++) {
              int val = innerFwdReader.getInt(i, context);
              Assert.assertEquals(val, -2147483648);
            }
            Assert.assertEquals(innerFwdReader.getInt(15, context), 1500);
            Assert.assertEquals(innerFwdReader.getInt(16, context), 1600);
            Assert.assertEquals(innerFwdReader.getInt(17, context), 1700);
            for (int i = 18; i < records.size(); i++) {
              int val = innerFwdReader.getInt(i, context);
              Assert.assertEquals(val, -2147483648);
            }

            Assert.assertEquals(innerFwdReader.getInt(15, context), 1500);
          } else {
            Assert.fail();
          }
        }
      } catch (Exception ex) {
        throw ex;
      }
    } catch (Exception ex) {
      Assert.fail("Error Creating Index", ex);
    }
  }

  @Test
  public void testingWithStringType() {
    // Create test data
    List<DimensionFieldSpec> keys = List.of(
        new DimensionFieldSpec("a", FieldSpec.DataType.STRING, true)
    );
    List<HashMap<String, Object>> records = createTestData(keys, 2000);

    // Configure map index
    MapIndexConfig config = new MapIndexConfig();
    config.setDenseKeys(keys);
    config.setMaxKeys(4);
    try {
      createIndex(config, records);
    } catch (Exception ex) {
      Assert.fail("Error Creating Index", ex);
    }
  }

  @Test
  public void testingWithDifferentTypes() {
    // Create test data
    List<DimensionFieldSpec> keys = List.of(
        new DimensionFieldSpec("a", FieldSpec.DataType.INT, true),
        new DimensionFieldSpec("b", FieldSpec.DataType.LONG, true),
        new DimensionFieldSpec("c", FieldSpec.DataType.FLOAT, true),
        new DimensionFieldSpec("d", FieldSpec.DataType.DOUBLE, true),
        new DimensionFieldSpec("e", FieldSpec.DataType.BOOLEAN, true),
        new DimensionFieldSpec("f", FieldSpec.DataType.STRING, true)
    );
    List<HashMap<String, Object>> records = createTestData(keys, 2000);

    // Configure map index
    MapIndexConfig config = new MapIndexConfig();
    config.setDenseKeys(keys);
    config.setMaxKeys(4);
    try {
      createIndex(config, records);
    } catch (Exception ex) {
      Assert.fail("Error Creating Index", ex);
    }
  }

  @Test
  public void typeMismatch() {
    // If a key from the input Map Value has a type that differs from the already defined type of the dense key then
    // Write the default value to the column.
    List<DimensionFieldSpec> keys = List.of(
        new DimensionFieldSpec("a", FieldSpec.DataType.STRING, true),
        new DimensionFieldSpec("b", FieldSpec.DataType.INT, true)
    );
    List<HashMap<String, Object>> records = createTestData(keys, 2000);
    MapIndexConfig config = new MapIndexConfig();
    config.setDenseKeys(keys);
    config.setMaxKeys(4);
    try {
      createIndex(config, records);
    } catch (Exception ex) {
      Assert.fail("Error Creating Index", ex);
    }
  }

  private List<HashMap<String, Object>> createTestData(List<DimensionFieldSpec> keys, int numRecords) {
    ArrayList<HashMap<String, Object>> records = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      HashMap<String, Object> record = new HashMap<>();

      for (int col = 0; col < keys.size(); col++) {
        record.put(keys.get(col).getName(), generateTestValue(keys.get(col).getDataType().getStoredType(), i));
      }

      records.add(record);
    }

    return records;
  }

  private Object generateTestValue(FieldSpec.DataType type, int i) {
    switch (type) {
      case INT:
        return i;
      case LONG:
        return 2L + (long) i;
      case FLOAT:
        return 3.0F + (float) i;
      case DOUBLE:
        return 4.5D + (double) i;
      case BOOLEAN:
        return i % 2 == 0;
      case TIMESTAMP:
      case STRING:
        return "hello" + i;
      case JSON:
      case BIG_DECIMAL:
      case BYTES:
      case STRUCT:
      case MAP:
      case LIST:
      case UNKNOWN:
      default:
        throw new UnsupportedOperationException();
    }
  }

  private void testForwardIndexReader(String key, ForwardIndexReader<ForwardIndexReaderContext> reader,
      List<HashMap<String, Object>> records) {
    ForwardIndexReaderContext context = reader.createContext();
    for (int docId = 0; docId < records.size(); docId++) {
      int expectedValue = (Integer) records.get(docId).get(key);
      int actualValue = reader.getInt(docId, context);
      Assert.assertEquals(actualValue, expectedValue, String.format("Mismatch for DocId: %d", docId));
    }
  }

  /**
   * Creates a Dense Map index with the given config and adds the given records
   * @param records
   * @throws IOException on error
   */
  private String createIndex(MapIndexConfig mapIndexConfig, List<HashMap<String, Object>> records)
      throws IOException {
    MapColumnStatistics stats = createMapColumnStats(records);
    DimensionFieldSpec mapSpec = new DimensionFieldSpec();
    mapSpec.setDataType(FieldSpec.DataType.MAP);
    mapSpec.setName(MAP_COLUMN_NAME);
    IndexCreationContext context =
        new IndexCreationContext.Common.Builder().withIndexDir(INDEX_DIR).withTotalDocs(records.size())
            .withLengthOfLongestEntry(30).sorted(false).onHeap(false).withDictionary(false).withFieldSpec(mapSpec)
            .withColumnStatistics(stats)
            .build();
    try (org.apache.pinot.segment.spi.index.creator.MapIndexCreator indexCreator = new MapIndexCreator(context,
        MAP_COLUMN_NAME, mapIndexConfig)) {
      for (int i = 0; i < records.size(); i++) {
        indexCreator.add(records.get(i));
      }
      indexCreator.seal();
    }

    String mapIndexFilePath =
        String.format("%s/%s%s", INDEX_DIR, MAP_COLUMN_NAME, MAP_FORWARD_INDEX_FILE_EXTENSION);
    return mapIndexFilePath;
  }

  private MapColumnStatistics createMapColumnStats(List<HashMap<String, Object>> records) {
    TestMapColStatistics stats = new TestMapColStatistics();

    for (HashMap<String, Object> record : records) {
      for (Map.Entry<String, Object> entry : record.entrySet()) {
        stats.recordValue(entry.getKey(), (Comparable) entry.getValue());
      }
    }

    return stats;
  }

  static class TestMapColStatistics implements MapColumnStatistics {
    private HashMap<String, Comparable> _minValueByKey;
    private HashMap<String, Comparable> _maxValueByKey;
    private HashMap<String, Integer> _shortestLengthByKey;
    private HashMap<String, Integer> _longestLengthByKey;

    public TestMapColStatistics() {
      _minValueByKey = new HashMap<>();
      _maxValueByKey = new HashMap<>();
      _shortestLengthByKey = new HashMap<>();
      _longestLengthByKey = new HashMap<>();
    }

    public void recordValue(String key, Comparable value) {
      if (_minValueByKey.containsKey(key)) {
        if (value.compareTo(_minValueByKey.get(key)) < 0) {
          _minValueByKey.put(key, value);
        }
      } else {
        _minValueByKey.put(key, value);
      }

      if (_maxValueByKey.containsKey(key)) {
        if (value.compareTo(_maxValueByKey.get(key)) > 0) {
          _maxValueByKey.put(key, value);
        }
      } else {
        _maxValueByKey.put(key, value);
      }

      // Get the length of the value
      if (value instanceof Integer) {
        _shortestLengthByKey.put(key, Integer.BYTES);
        _longestLengthByKey.put(key, Integer.BYTES);
      } else if (value instanceof Long) {
        _shortestLengthByKey.put(key, Long.BYTES);
        _longestLengthByKey.put(key, Long.BYTES);
      } else if (value instanceof Float) {
        _shortestLengthByKey.put(key, Float.BYTES);
        _longestLengthByKey.put(key, Float.BYTES);
      } else if (value instanceof Double) {
        _shortestLengthByKey.put(key, Double.BYTES);
        _longestLengthByKey.put(key, Double.BYTES);
      } else if (value instanceof String) {
        int length = ((String) value).length();
        if (_shortestLengthByKey.containsKey(key)) {
          if (length < _shortestLengthByKey.get(key)) {
            _shortestLengthByKey.put(key, length);
          }
        } else {
          _shortestLengthByKey.put(key, length);
        }

        if (_longestLengthByKey.containsKey(key)) {
          if (length > _longestLengthByKey.get(key)) {
            _longestLengthByKey.put(key, length);
          }
        } else {
          _longestLengthByKey.put(key, length);
        }
      }
    }

    @Override
    public Object getMinValue() {
      return null;
    }

    @Override
    public Object getMaxValue() {
      return null;
    }

    @Override
    public Object getUniqueValuesSet() {
      return null;
    }

    @Override
    public int getCardinality() {
      return 0;
    }

    @Override
    public int getLengthOfShortestElement() {
      return 0;
    }

    @Override
    public int getLengthOfLargestElement() {
      return 0;
    }

    @Override
    public boolean isSorted() {
      return false;
    }

    @Override
    public int getTotalNumberOfEntries() {
      return 0;
    }

    @Override
    public int getMaxNumberOfMultiValues() {
      return 0;
    }

    @Override
    public PartitionFunction getPartitionFunction() {
      return null;
    }

    @Override
    public int getNumPartitions() {
      return 0;
    }

    @Override
    public Map<String, String> getPartitionFunctionConfig() {
      return null;
    }

    @Override
    public Set<Integer> getPartitions() {
      return null;
    }

    @Override
    public Object getMinValueForKey(String key) {
      return null;
    }

    @Override
    public Object getMaxValueForKey(String key) {
      return null;
    }

    @Override
    public int getLengthOfShortestElementForKey(String key) {
      return 0;
    }

    @Override
    public int getLengthOfLargestElementForKey(String key) {
      return 0;
    }

    @Override
    public Set<Pair<String, FieldSpec.DataType>> getKeys() {
      return null;
    }

    @Override
    public boolean isSortedForKey(String key) {
      return false;
    }

    @Override
    public int getTotalNumberOfEntriesForKey(String key) {
      return 0;
    }
  }
}
