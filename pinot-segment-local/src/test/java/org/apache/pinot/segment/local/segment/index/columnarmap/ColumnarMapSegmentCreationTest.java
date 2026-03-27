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
package org.apache.pinot.segment.local.segment.index.columnarmap;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests full segment creation pipeline for MAP columns with sparse map index.
 * Verifies that OnHeapColumnarMapIndexCreator is properly invoked and produces an index file.
 */
public class ColumnarMapSegmentCreationTest {

  private static final File SEGMENT_DIR = new File(FileUtils.getTempDirectory(), "ColumnarMapSegmentCreationTest");
  private static final String TABLE_NAME = "userMetrics";

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.forceMkdir(SEGMENT_DIR);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(SEGMENT_DIR);
  }

  @Test
  public void testColumnarMapIndexCreatedInSegment()
      throws Exception {
    // Build schema with a MAP column
    Map<String, FieldSpec.DataType> keyTypes = new HashMap<>();
    keyTypes.put("clicks", FieldSpec.DataType.LONG);
    keyTypes.put("spend", FieldSpec.DataType.DOUBLE);

    Map<String, FieldSpec> childFieldSpecs = Map.of(
        "key", new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true),
        "value", new DimensionFieldSpec("value", FieldSpec.DataType.STRING, true)
    );
    ComplexFieldSpec metricsSpec = new ComplexFieldSpec("metrics", FieldSpec.DataType.MAP, true, childFieldSpecs);

    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName(TABLE_NAME)
        .addField(new DimensionFieldSpec("userId", FieldSpec.DataType.STRING, true))
        .addField(metricsSpec)
        .build();

    // Build table config with columnar_map index in fieldConfigList
    ObjectNode columnarMapNode = JsonUtils.newObjectNode();
    columnarMapNode.put("enabled", true);
    columnarMapNode.put("enableInvertedIndexForAll", false);
    columnarMapNode.put("maxKeys", 100);
    ObjectNode indexesNode = JsonUtils.newObjectNode();
    indexesNode.set("columnar_map", columnarMapNode);
    FieldConfig metricsFieldConfig = new FieldConfig.Builder("metrics")
        .withIndexes(indexesNode)
        .build();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setFieldConfigList(List.of(metricsFieldConfig))
        .build();

    // Build test rows
    List<Map<String, Object>> rows = new ArrayList<>();
    rows.add(buildRow("user1", Map.of("clicks", 100L, "spend", 5.5)));
    rows.add(buildRow("user2", Map.of("clicks", 200L)));
    rows.add(buildRow("user3", Map.of("spend", 12.75)));
    rows.add(buildRow("user4", new HashMap<>()));

    // Create segment generator config
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setTableName(TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(TABLE_NAME + "_0");
    segmentGeneratorConfig.setOutDir(SEGMENT_DIR.getAbsolutePath());

    // Create segment with test reader
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new TestRecordReader(rows));
    driver.build();

    // Verify the segment was created
    File segmentDir = new File(SEGMENT_DIR, TABLE_NAME + "_0");
    assertTrue(segmentDir.exists(), "Segment directory should exist: " + segmentDir);

    // Verify the sparse map index file exists in the V3 segment
    File v3Dir = new File(segmentDir, "v3");
    assertTrue(v3Dir.exists(), "V3 directory should exist: " + v3Dir);

    File indexMap = new File(v3Dir, "index_map");
    assertTrue(indexMap.exists(), "index_map should exist");

    String indexMapContent = org.apache.commons.io.FileUtils.readFileToString(indexMap,
        java.nio.charset.StandardCharsets.UTF_8);

    // The sparse map index should be present in the index_map
    assertTrue(indexMapContent.contains("metrics." + StandardIndexes.COLUMNAR_MAP_ID),
        "index_map should contain metrics columnar_map_index entry but got:\n" + indexMapContent);
  }

  private Map<String, Object> buildRow(String userId, Map<String, Object> metrics) {
    Map<String, Object> row = new HashMap<>();
    row.put("userId", userId);
    row.put("metrics", metrics);
    return row;
  }

  /**
   * Simple in-memory record reader for testing.
   */
  private static class TestRecordReader implements RecordReader {
    private final List<Map<String, Object>> _rows;
    private int _index = 0;

    TestRecordReader(List<Map<String, Object>> rows) {
      _rows = rows;
    }

    @Override
    public void init(File dataFile, java.util.Set<String> fieldsToRead,
        org.apache.pinot.spi.data.readers.RecordReaderConfig recordReaderConfig)
        throws java.io.IOException {
    }

    @Override
    public boolean hasNext() {
      return _index < _rows.size();
    }

    @Override
    public GenericRow next(GenericRow reuse)
        throws java.io.IOException {
      Map<String, Object> rowData = _rows.get(_index++);
      reuse.clear();
      for (Map.Entry<String, Object> entry : rowData.entrySet()) {
        reuse.putValue(entry.getKey(), entry.getValue());
      }
      return reuse;
    }

    @Override
    public void rewind()
        throws java.io.IOException {
      _index = 0;
    }

    @Override
    public void close() {
    }
  }
}
