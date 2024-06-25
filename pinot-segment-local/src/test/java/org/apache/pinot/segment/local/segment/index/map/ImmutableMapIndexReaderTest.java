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
import org.apache.pinot.segment.local.segment.creator.impl.map.MapIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.map.ImmutableMapIndexReader;
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


public class ImmutableMapIndexReaderTest {
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
        new DimensionFieldSpec("foo", FieldSpec.DataType.INT, true),
        new DimensionFieldSpec("barry", FieldSpec.DataType.INT, true));
    List<HashMap<String, Object>> records = createTestData(keys, 2000);
    MapIndexConfig config = new MapIndexConfig();
    config.setDenseKeys(keys);
    config.setMaxKeys(4);
    try {
      String mapIndexFilePath = createIndex(config, records);

      File mapIndexFile = new File(mapIndexFilePath);
      long size = Files.size(mapIndexFile.toPath());
      try (PinotDataBuffer mapBuffer = PinotDataBuffer.mapFile(mapIndexFile, true, 0, size, ByteOrder.BIG_ENDIAN,
          "test")) {
            ImmutableMapIndexReader mapReader = new ImmutableMapIndexReader(mapBuffer);
            ForwardIndexReader aReader = (ForwardIndexReader) mapReader.getKeyReader("foo", StandardIndexes.forward());

            ForwardIndexReaderContext context = aReader.createContext();
            int doc0 = aReader.getInt(0, context);
            Assert.assertEquals(doc0, 1);
        } catch (Exception ex) {
        throw ex;
      }
    } catch (Exception ex) {
      Assert.fail("Error Creating Index", ex);
    }
  }

  @Test
  public void testingWithDoubleType() {
    List<DimensionFieldSpec> keys = List.of(
        new DimensionFieldSpec("foo", FieldSpec.DataType.DOUBLE, true),
        new DimensionFieldSpec("barry", FieldSpec.DataType.DOUBLE, true));
    List<HashMap<String, Object>> records = createTestData(keys, 2000);
    MapIndexConfig config = new MapIndexConfig();
    config.setDenseKeys(keys);
    config.setMaxKeys(4);
    try {
      String mapIndexFilePath = createIndex(config, records);

      File mapIndexFile = new File(mapIndexFilePath);
      long size = Files.size(mapIndexFile.toPath());
      try (PinotDataBuffer mapBuffer = PinotDataBuffer.mapFile(mapIndexFile, true, 0, size, ByteOrder.BIG_ENDIAN,
          "test")) {
        ImmutableMapIndexReader mapReader = new ImmutableMapIndexReader(mapBuffer);

        ForwardIndexReader aReader = (ForwardIndexReader) mapReader.getKeyReader("foo", StandardIndexes.forward());
        testForwardIndexReader("foo", aReader, records);

        ForwardIndexReader bReader = (ForwardIndexReader) mapReader.getKeyReader("barry", StandardIndexes.forward());
        testForwardIndexReader("barry", bReader, records);
      } catch (Exception ex) {
        throw ex;
      }
    } catch (Exception ex) {
      Assert.fail("Error Creating Index", ex);
    }
  }

  @Test
  public void testingWithMixedTypes() {
    List<DimensionFieldSpec> keys = List.of(
        new DimensionFieldSpec("foo", FieldSpec.DataType.INT, true),
        new DimensionFieldSpec("barry", FieldSpec.DataType.LONG, true),
        new DimensionFieldSpec("c", FieldSpec.DataType.FLOAT, true),
        new DimensionFieldSpec("d", FieldSpec.DataType.DOUBLE, true)
    );
    List<HashMap<String, Object>> records = createTestData(keys, 2000);
    MapIndexConfig config = new MapIndexConfig();
    config.setDenseKeys(keys);
    config.setMaxKeys(4);
    try {
      String mapIndexFilePath = createIndex(config, records);

      File mapIndexFile = new File(mapIndexFilePath);
      long size = Files.size(mapIndexFile.toPath());
      try (PinotDataBuffer mapBuffer = PinotDataBuffer.mapFile(mapIndexFile, true, 0, size, ByteOrder.BIG_ENDIAN,
          "test")) {
        ImmutableMapIndexReader mapReader = new ImmutableMapIndexReader(mapBuffer);

        for (FieldSpec key : keys) {
          ForwardIndexReader keyReader = (ForwardIndexReader) mapReader.getKeyReader(key.getName(),
              StandardIndexes.forward());
          testForwardIndexReader(key.getName(), keyReader, records);
        }
      } catch (Exception ex) {
        throw ex;
      }
    } catch (Exception ex) {
      Assert.fail("Error Creating Index", ex);
    }
  }

  private List<HashMap<String, Object>> createTestData(List<DimensionFieldSpec> keys, int numRecords) {
    HashMap<String, Object> record = new HashMap<>();

    for (int i = 0; i < keys.size(); i++) {
      record.put(keys.get(i).getName(), generateTestValue(keys.get(i).getDataType().getStoredType()));
    }

    ArrayList<HashMap<String, Object>> records = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      records.add(record);
    }

    return records;
  }

  private Object generateTestValue(FieldSpec.DataType type) {
    switch (type) {
      case INT:
        return 1;
      case LONG:
        return 2L;
      case FLOAT:
        return 3.0F;
      case DOUBLE:
        return 4.5D;
      case BOOLEAN:
        return true;
      case TIMESTAMP:
      case STRING:
        return "hello";
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
    FieldSpec.DataType type = reader.getStoredType();
    ForwardIndexReaderContext context = reader.createContext();
    for (int docId = 0; docId < records.size(); docId++) {
      switch (type) {
        case INT: {
          int expectedValue = (Integer) records.get(docId).get(key);
          int actualValue = reader.getInt(docId, context);
          Assert.assertEquals(actualValue, expectedValue, String.format("Mismatch for DocId: %d", docId));
          break;
        }
        case LONG: {
          long expectedValue = (Long) records.get(docId).get(key);
          long actualValue = reader.getLong(docId, context);
          Assert.assertEquals(actualValue, expectedValue, String.format("Mismatch for DocId: %d", docId));
          break;
        }
        case FLOAT: {
          float expectedValue = (Float) records.get(docId).get(key);
          float actualValue = reader.getFloat(docId, context);
          Assert.assertEquals(actualValue, expectedValue, String.format("Mismatch for DocId: %d", docId));
          break;
        }
        case DOUBLE: {
          double expectedValue = (Double) records.get(docId).get(key);
          double actualValue = reader.getDouble(docId, context);
          Assert.assertEquals(actualValue, expectedValue, String.format("Mismatch for DocId: %d", docId));
          break;
        }
        case BIG_DECIMAL:
        case BOOLEAN:
        case TIMESTAMP:
        case STRING:
        case JSON:
        case BYTES:
        case STRUCT:
        case MAP:
        case LIST:
        case UNKNOWN:
        default:
          throw new UnsupportedOperationException();
      }
    }
  }

  /**
   * Creates a Dense Map index with the given config and adds the given records
   * @param records
   * @throws IOException on error
   */
  private String createIndex(MapIndexConfig mapIndexConfig, List<HashMap<String, Object>> records)
      throws IOException {
    DimensionFieldSpec mapSpec = new DimensionFieldSpec();
    mapSpec.setDataType(FieldSpec.DataType.MAP);
    mapSpec.setName(MAP_COLUMN_NAME);

    IndexCreationContext context =
        new IndexCreationContext.Common.Builder().withIndexDir(INDEX_DIR).withTotalDocs(records.size())
            .withColumnStatistics(createMapColumnStats(records))
            .withLengthOfLongestEntry(30).sorted(false).onHeap(false).withDictionary(false).withFieldSpec(mapSpec)
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
