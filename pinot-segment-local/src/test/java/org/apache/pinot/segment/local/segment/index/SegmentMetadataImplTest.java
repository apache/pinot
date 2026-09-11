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
package org.apache.pinot.segment.local.segment.index;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.OpenStructNaming;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class SegmentMetadataImplTest {
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "SegmentMetadataImplTest");
  private File _avroFile;
  private File _segmentDirectory;

  @BeforeMethod
  public void setUp()
      throws Exception {
    final String filePath =
        TestUtils.getFileFromResourceUrl(SegmentMetadataImplTest.class.getClassLoader().getResource(AVRO_DATA));
    _avroFile = new File(filePath);

    // intentionally changed this to TimeUnit.Hours to make it non-default for testing
    final SegmentGeneratorConfig config = SegmentTestUtils
        .getSegmentGenSpecWithSchemAndProjectedColumns(_avroFile, INDEX_DIR, "daysSinceEpoch", TimeUnit.HOURS,
            "testTable");
    config.setSegmentNamePostfix("1");
    config.setCustomProperties(Map.of("custom.k1", "v1", "custom.k2", "v2"));
    final SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();
    _segmentDirectory = new File(INDEX_DIR, driver.getSegmentName());
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_segmentDirectory);
  }

  @Test
  public void testToJson()
      throws IOException, ConfigurationException {
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(_segmentDirectory);
    Assert.assertNotNull(metadata);

    JsonNode jsonMeta = metadata.toJson(null);
    assertEquals(jsonMeta.get("segmentName").asText(), metadata.getName());
    Assert.assertEquals(jsonMeta.get("crc").asLong(), Long.valueOf(metadata.getCrc()).longValue());
    Assert.assertTrue(jsonMeta.get("creatorName").isNull());
    assertEquals(jsonMeta.get("creationTimeMillis").asLong(), metadata.getIndexCreationTime());
    assertEquals(jsonMeta.get("timeColumn").asText(), metadata.getTimeColumn());
    assertEquals(jsonMeta.get("timeUnit").asText(), metadata.getTimeUnit().name());
    assertEquals(jsonMeta.get("startTimeMillis").asLong(), metadata.getTimeInterval().getStartMillis());
    assertEquals(jsonMeta.get("endTimeMillis").asLong(), metadata.getTimeInterval().getEndMillis());
    assertEquals(jsonMeta.get("totalDocs").asInt(), metadata.getTotalDocs());
    assertEquals(jsonMeta.get("custom").get("k1").asText(), metadata.getCustomMap().get("k1"));
    assertEquals(jsonMeta.get("custom").get("k2").asText(), metadata.getCustomMap().get("k2"));

    JsonNode jsonColumnsMeta = jsonMeta.get("columns");
    int numColumns = jsonColumnsMeta.size();
    assertEquals(numColumns, metadata.getAllColumns().size());
    for (int i = 0; i < numColumns; i++) {
      JsonNode jsonColumnMeta = jsonColumnsMeta.get(i);
      ColumnMetadata columnMeta = metadata.getColumnMetadataFor(jsonColumnMeta.get("columnName").asText());
      Assert.assertNotNull(columnMeta);
      assertEquals(jsonColumnMeta.get("cardinality").asInt(), columnMeta.getCardinality());
      assertEquals(jsonColumnMeta.get("bitsPerElement").asInt(), columnMeta.getBitsPerElement());
      assertEquals(jsonColumnMeta.get("sorted").asBoolean(), columnMeta.isSorted());
      assertEquals(jsonColumnMeta.get("hasDictionary").asBoolean(), columnMeta.hasDictionary());
    }
  }

  /// Index sizes come from the local `index_map`, so a segment loaded through the stream constructor (tiered
  /// storage, no index directory) reports none while the rest of the metadata matches the directory load.
  @Test
  public void testIndexSizesOnlyFromIndexDir()
      throws Exception {
    // The fixture builds a v1 segment; index sizes exist only in the v3 index_map.
    new SegmentV1V2ToV3FormatConverter().convert(_segmentDirectory);
    SegmentMetadataImpl fromDir = new SegmentMetadataImpl(_segmentDirectory);
    assertEquals(fromDir.getVersion(), SegmentVersion.v3);
    SegmentMetadataImpl fromStreams;
    try (FileInputStream metadataProperties =
        new FileInputStream(SegmentDirectoryPaths.findMetadataFile(_segmentDirectory));
        FileInputStream creationMeta =
            new FileInputStream(SegmentDirectoryPaths.findCreationMetaFile(_segmentDirectory))) {
      fromStreams = new SegmentMetadataImpl(metadataProperties, creationMeta);
    }

    assertEquals(fromStreams.getColumnMetadataMap().keySet(), fromDir.getColumnMetadataMap().keySet());
    assertEquals(fromStreams.getTotalDocs(), fromDir.getTotalDocs());
    assertEquals(fromStreams.getSchema(), fromDir.getSchema());
    assertSame(fromStreams.getTimeColumn(), fromDir.getTimeColumn());
    for (Map.Entry<String, ColumnMetadata> entry : fromDir.getColumnMetadataMap().entrySet()) {
      ColumnMetadata dirColumn = entry.getValue();
      assertTrue(dirColumn.getNumIndexes() > 0, entry.getKey());
      long forwardSize = dirColumn.getIndexSizeFor(StandardIndexes.forward());
      assertTrue(forwardSize > 0, entry.getKey());
      assertEquals(((ColumnMetadataImpl) dirColumn).getIndexSizeMap().get(StandardIndexes.forward()),
          (Long) forwardSize, entry.getKey());
      ColumnMetadata streamColumn = fromStreams.getColumnMetadataMap().get(entry.getKey());
      assertEquals(streamColumn.getNumIndexes(), 0, entry.getKey());
      assertEquals(streamColumn.getIndexSizeFor(StandardIndexes.forward()), ColumnMetadata.UNAVAILABLE,
          entry.getKey());
      assertTrue(((ColumnMetadataImpl) streamColumn).getIndexSizeMap().isEmpty(), entry.getKey());
    }
  }

  /// A server holds one column-metadata graph per loaded segment, so the per-column strings and default null values
  /// are shared: two loads of the same metadata alias one interned column name (as the map key, the FieldSpec name and
  /// the Schema entry) and hold the static FieldSpec default constant rather than a box parsed from the literal the
  /// segment creator wrote, while the specs stay equal to the schema the segment was built from.
  @Test
  public void testColumnStringsAndDefaultsSharedAcrossLoads()
      throws Exception {
    SegmentMetadataImpl first = new SegmentMetadataImpl(_segmentDirectory);
    SegmentMetadataImpl second = new SegmentMetadataImpl(_segmentDirectory);
    assertEquals(first.getTimeColumn(), "daysSinceEpoch");
    assertSame(first.getTimeColumn(), second.getTimeColumn());
    assertSame(first.getTimeColumn(), first.getColumnMetadataMap().ceilingKey(first.getTimeColumn()));
    assertEquals(first.getColumnMetadataMap().keySet(), second.getColumnMetadataMap().keySet());
    assertSame(first.getColumnMetadataMap().firstKey(), second.getColumnMetadataMap().firstKey());
    assertSame(first.getSchema().getDimensionNames().get(0), second.getSchema().getDimensionNames().get(0));
    Iterator<String> secondKeys = second.getColumnMetadataMap().keySet().iterator();
    for (String column : first.getColumnMetadataMap().keySet()) {
      assertSame(column, secondKeys.next());
      FieldSpec fieldSpec = first.getColumnMetadataFor(column).getFieldSpec();
      assertSame(fieldSpec.getName(), column, column);
      assertSame(fieldSpec.getName(), second.getColumnMetadataFor(column).getFieldSpec().getName(), column);
      assertSame(first.getSchema().getFieldSpecFor(column).getName(), column, column);
      assertSame(fieldSpec.getDefaultNullValue(),
          FieldSpec.getDefaultNullValue(fieldSpec.getFieldType(), fieldSpec.getDataType(), null), column);
    }
    Schema inputSchema = SegmentTestUtils.extractSchemaFromAvroWithoutTime(_avroFile);
    for (FieldSpec inputFieldSpec : inputSchema.getAllFieldSpecs()) {
      assertEquals(first.getSchema().getFieldSpecFor(inputFieldSpec.getName()), inputFieldSpec);
    }
  }

  /// OPEN_STRUCT children carry an explicit column name and a parent column in `metadata.properties`; both come back
  /// as the interned instances, so the child's parent name is the very String that keys the parent column.
  @Test
  public void testOpenStructChildStringsShared()
      throws Exception {
    String parent = "metrics";
    File segmentDir = buildOpenStructSegment(parent);
    try {
      SegmentMetadataImpl first = new SegmentMetadataImpl(segmentDir);
      SegmentMetadataImpl second = new SegmentMetadataImpl(segmentDir);
      assertNull(first.getTimeColumn());
      assertNull(second.getTimeColumn());
      String parentKey = first.getColumnMetadataMap().ceilingKey(parent);
      assertEquals(parentKey, parent);
      assertSame(parentKey, second.getColumnMetadataMap().ceilingKey(parent));
      String child = OpenStructNaming.materializedColumnName(parent, "cpu");
      ColumnMetadataImpl firstChild = (ColumnMetadataImpl) first.getColumnMetadataFor(child);
      ColumnMetadataImpl secondChild = (ColumnMetadataImpl) second.getColumnMetadataFor(child);
      assertEquals(firstChild.getParentColumn(), parent);
      assertSame(firstChild.getParentColumn(), parentKey);
      assertSame(firstChild.getParentColumn(), secondChild.getParentColumn());
      assertSame(firstChild.getFieldSpec().getName(), secondChild.getFieldSpec().getName());
      assertSame(firstChild.getFieldSpec().getDefaultNullValue(), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE);
      ComplexFieldSpec firstParent = (ComplexFieldSpec) first.getColumnMetadataFor(parent).getFieldSpec();
      ComplexFieldSpec secondParent = (ComplexFieldSpec) second.getColumnMetadataFor(parent).getFieldSpec();
      assertEquals(firstParent.getChildFieldSpecs().keySet(), Set.of("views", "cpu", "host"));
      for (Map.Entry<String, FieldSpec> entry : firstParent.getChildFieldSpecs().entrySet()) {
        FieldSpec childSpec = entry.getValue();
        assertSame(childSpec.getName(), entry.getKey());
        assertSame(childSpec.getName(), secondParent.getChildFieldSpec(entry.getKey()).getName());
        assertSame(childSpec.getDefaultNullValue(),
            FieldSpec.getDefaultNullValue(childSpec.getFieldType(), childSpec.getDataType(), null));
      }
    } finally {
      FileUtils.deleteQuietly(segmentDir);
    }
  }

  private static File buildOpenStructSegment(String parent)
      throws Exception {
    Map<String, FieldSpec> children = new HashMap<>();
    children.put("views", new DimensionFieldSpec("views", FieldSpec.DataType.LONG, true));
    children.put("cpu", new DimensionFieldSpec("cpu", FieldSpec.DataType.DOUBLE, true));
    children.put("host", new DimensionFieldSpec("host", FieldSpec.DataType.STRING, true));
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testOpenStruct")
        .addField(new ComplexFieldSpec(parent, FieldSpec.DataType.OPEN_STRUCT, true, children))
        .addSingleValueDimension("dim", FieldSpec.DataType.STRING)
        .build();
    OpenStructIndexConfig openStructConfig =
        new OpenStructIndexConfig(false, null, 3, Set.of("views", "cpu", "host"), 0.5, List.of(), null);
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("open_struct", JsonUtils.objectToJsonNode(openStructConfig));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testOpenStruct")
        .setFieldConfigList(List.of(new FieldConfig.Builder(parent).withIndexes(indexes).build())).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(new File(INDEX_DIR, "openStruct").getAbsolutePath());
    config.setSegmentName("openStructSegment");
    List<GenericRow> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      GenericRow row = new GenericRow();
      Map<String, Object> metrics = new HashMap<>();
      metrics.put("views", (long) i);
      metrics.put("cpu", i * 0.5);
      metrics.put("host", "host-" + i);
      row.putValue(parent, metrics);
      row.putValue("dim", "val-" + i);
      rows.add(row);
    }
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return driver.getOutputDirectory();
  }
}
