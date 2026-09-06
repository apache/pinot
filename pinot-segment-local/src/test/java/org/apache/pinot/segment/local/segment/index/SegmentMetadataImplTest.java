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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.metadata.EmptyColumnMetadata;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
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
import org.apache.pinot.spi.utils.CommonConstants.Segment.BuiltInVirtualColumn;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.NetUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


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

  /// Every segment of a table parses the same column definitions, so the FieldSpecs are interned: two loads alias one
  /// instance per column, in the column metadata and in the segment Schema alike, while the Schema object itself stays
  /// per segment and the specs stay equal to the schema the segment was built from.
  @Test
  public void testFieldSpecsSharedAcrossLoads()
      throws Exception {
    SegmentMetadataImpl first = new SegmentMetadataImpl(_segmentDirectory);
    SegmentMetadataImpl second = new SegmentMetadataImpl(_segmentDirectory);
    assertEquals(first.getSchema(), second.getSchema());
    assertNotSame(first.getSchema(), second.getSchema());
    for (String column : first.getColumnMetadataMap().keySet()) {
      FieldSpec fieldSpec = first.getColumnMetadataFor(column).getFieldSpec();
      assertSame(second.getColumnMetadataFor(column).getFieldSpec(), fieldSpec, column);
      assertSame(first.getSchema().getFieldSpecFor(column), fieldSpec, column);
      assertSame(second.getSchema().getFieldSpecFor(column), fieldSpec, column);
    }
    Schema inputSchema = SegmentTestUtils.extractSchemaFromAvroWithoutTime(_avroFile);
    for (FieldSpec inputFieldSpec : inputSchema.getAllFieldSpecs()) {
      assertEquals(first.getSchema().getFieldSpecFor(inputFieldSpec.getName()), inputFieldSpec);
    }

    // Only the specs are shared: removing a column from one segment's schema leaves the other segment intact.
    String column = first.getColumnMetadataMap().firstKey();
    first.getSchema().removeField(column);
    assertNull(first.getSchema().getFieldSpecFor(column));
    assertNotNull(second.getSchema().getFieldSpecFor(column));
    assertSame(second.getSchema().getFieldSpecFor(column), second.getColumnMetadataFor(column).getFieldSpec());
  }

  /// A COMPLEX parent is not interned (ComplexFieldSpec does not override equals, so two structs with different
  /// children would alias), but its children and the materialized child columns are.
  @Test
  public void testOpenStructChildSpecsSharedButParentIsNot()
      throws Exception {
    String parent = "metrics";
    File segmentDir = buildOpenStructSegment(parent);
    try {
      SegmentMetadataImpl first = new SegmentMetadataImpl(segmentDir);
      SegmentMetadataImpl second = new SegmentMetadataImpl(segmentDir);
      ComplexFieldSpec firstParent = (ComplexFieldSpec) first.getColumnMetadataFor(parent).getFieldSpec();
      ComplexFieldSpec secondParent = (ComplexFieldSpec) second.getColumnMetadataFor(parent).getFieldSpec();
      assertNotSame(secondParent, firstParent);
      assertEquals(secondParent.getChildFieldSpecs(), firstParent.getChildFieldSpecs());
      for (Map.Entry<String, FieldSpec> entry : firstParent.getChildFieldSpecs().entrySet()) {
        assertSame(secondParent.getChildFieldSpec(entry.getKey()), entry.getValue(), entry.getKey());
      }
      String child = OpenStructNaming.materializedColumnName(parent, "cpu");
      assertSame(second.getColumnMetadataFor(child).getFieldSpec(), first.getColumnMetadataFor(child).getFieldSpec());
      assertSame(second.getColumnMetadataFor("dim").getFieldSpec(), first.getColumnMetadataFor("dim").getFieldSpec());
    } finally {
      FileUtils.deleteQuietly(segmentDir);
    }
  }

  /// A server retains the metadata of every loaded segment, and a Schema costs a TreeMap entry plus list slots per
  /// column on top of the FieldSpecs the column metadata already holds, so the schema is derived on the first
  /// getSchema() rather than at load. It equals the one that used to be built eagerly, is built exactly once, and
  /// listing the columns or rendering the metadata JSON does not build it.
  @Test
  public void testSchemaDerivedLazilyFromColumnMetadata()
      throws Exception {
    long materializations = SegmentMetadataImpl.getNumSchemaMaterializations();
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(_segmentDirectory);
    assertFalse(metadata.isSchemaMaterialized());
    assertEquals(metadata.getAllColumns(), metadata.getColumnMetadataMap().keySet());
    assertEquals(metadata.toJson(null).get("columns").size(), metadata.getAllColumns().size());
    assertTrue(metadata.toJson(null).get("schemaName").isNull());
    assertFalse(metadata.isSchemaMaterialized());
    assertEquals(SegmentMetadataImpl.getNumSchemaMaterializations(), materializations);

    Schema eager = new Schema();
    for (ColumnMetadata columnMetadata : metadata.getColumnMetadataMap().values()) {
      eager.addField(columnMetadata.getFieldSpec());
    }
    Schema schema = metadata.getSchema();
    assertTrue(metadata.isSchemaMaterialized());
    assertEquals(SegmentMetadataImpl.getNumSchemaMaterializations(), materializations + 1);
    assertEquals(schema, eager);
    assertEquals(schema.getColumnNames(), metadata.getAllColumns());
    for (String column : metadata.getAllColumns()) {
      assertSame(schema.getFieldSpecFor(column), metadata.getColumnMetadataFor(column).getFieldSpec(), column);
    }
    assertSame(metadata.getSchema(), schema);
    assertEquals(SegmentMetadataImpl.getNumSchemaMaterializations(), materializations + 1);
  }

  /// The preprocess that runs on every segment load asks the forward-index handler which physical columns exist. That
  /// question must not build the per-segment schema: doing so once per segment pins one [Schema] per loaded segment
  /// for its whole life, which on a server holding tens of thousands of wide segments is hundreds of megabytes.
  @Test
  public void testPreprocessDoesNotBuildTheSegmentSchema()
      throws Exception {
    // The forward-index handler skips segments older than v3, so the preprocess only reaches it on a v3 segment.
    new SegmentV1V2ToV3FormatConverter().convert(_segmentDirectory);

    long materializations = SegmentMetadataImpl.getNumSchemaMaterializations();
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(_segmentDirectory);
    Set<String> physical = metadata.getPhysicalColumnNames();
    assertFalse(metadata.isSchemaMaterialized(), "listing physical columns must not build the segment schema");
    assertEquals(SegmentMetadataImpl.getNumSchemaMaterializations(), materializations);
    assertEquals(physical, metadata.getSchema().getPhysicalColumnNames(),
        "the derived names must equal what the schema reports");
    assertFalse(physical.contains(BuiltInVirtualColumn.DOCID));

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setTimeColumnName("daysSinceEpoch").build();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, metadata.getSchema());
    indexLoadingConfig.setReadMode(ReadMode.mmap);
    long beforePreprocess = SegmentMetadataImpl.getNumSchemaMaterializations();
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(_segmentDirectory, ReadMode.mmap);
        SegmentPreProcessor preProcessor = new SegmentPreProcessor(segmentDirectory, indexLoadingConfig)) {
      preProcessor.process();
    }
    assertEquals(SegmentMetadataImpl.getNumSchemaMaterializations(), beforePreprocess,
        "segment preprocess must not build any segment schema");
  }

  /// Loading a segment registers the built-in virtual columns in the column metadata, so the schema derived afterwards
  /// includes them exactly as the schema the loader used to build eagerly did, while neither the load nor serving the
  /// segment (column listings, data sources, the metadata JSON) builds any schema.
  @Test
  public void testSchemaIncludesBuiltInVirtualColumnsAfterLoad()
      throws Exception {
    long materializations = SegmentMetadataImpl.getNumSchemaMaterializations();
    ImmutableSegment segment = ImmutableSegmentLoader.load(_segmentDirectory, ReadMode.mmap);
    try {
      SegmentMetadataImpl metadata = (SegmentMetadataImpl) segment.getSegmentMetadata();
      assertFalse(metadata.isSchemaMaterialized(), "the load path must not build the segment schema");
      assertTrue(segment.getColumnNames().containsAll(BuiltInVirtualColumn.BUILT_IN_VIRTUAL_COLUMNS));
      assertTrue(metadata.getAllColumns().containsAll(BuiltInVirtualColumn.BUILT_IN_VIRTUAL_COLUMNS));
      assertFalse(segment.getPhysicalColumnNames().contains(BuiltInVirtualColumn.DOCID));
      assertEquals(segment.getPhysicalColumnNames().size(),
          segment.getColumnNames().size() - BuiltInVirtualColumn.BUILT_IN_VIRTUAL_COLUMNS.size());
      for (String column : segment.getColumnNames()) {
        assertNotNull(segment.getDataSource(column), column);
      }
      metadata.toJson(null);
      assertFalse(metadata.isSchemaMaterialized(), "serving the segment must not build the segment schema");
      assertEquals(SegmentMetadataImpl.getNumSchemaMaterializations(), materializations);

      Schema legacy = new Schema();
      for (String column : segment.getPhysicalColumnNames()) {
        legacy.addField(metadata.getColumnMetadataFor(column).getFieldSpec());
      }
      VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSegmentSchema(legacy, metadata.getName());
      Schema schema = metadata.getSchema();
      assertEquals(schema, legacy);
      assertEquals(schema.getColumnNames(), metadata.getAllColumns());
      assertEquals(schema.getPhysicalColumnNames(), segment.getPhysicalColumnNames());
      for (String column : BuiltInVirtualColumn.BUILT_IN_VIRTUAL_COLUMNS) {
        FieldSpec fieldSpec = schema.getFieldSpecFor(column);
        assertTrue(fieldSpec.isVirtualColumn(), column);
        assertSame(fieldSpec, metadata.getColumnMetadataFor(column).getFieldSpec(), column);
      }
      assertEquals(schema.getFieldSpecFor(BuiltInVirtualColumn.SEGMENTNAME).getDefaultNullValue(),
          metadata.getName());
      assertEquals(schema.getFieldSpecFor(BuiltInVirtualColumn.HOSTNAME).getDefaultNullValue(),
          NetUtils.getHostnameOrAddress());
    } finally {
      segment.destroy();
    }
  }

  /// The columns are held as sorted arrays; the `TreeMap` view exists only for compatibility and costs a map entry
  /// per column, so it is derived on the first getColumnMetadataMap() and never by the accessors the load and query
  /// paths use.
  @Test
  public void testColumnMetadataMapDerivedLazily()
      throws Exception {
    long materializations = SegmentMetadataImpl.getNumColumnMetadataMapMaterializations();
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(_segmentDirectory);
    assertFalse(metadata.isColumnMetadataMapMaterialized());

    List<String> columns = new ArrayList<>(metadata.getAllColumns());
    assertEquals(metadata.getNumColumns(), columns.size());
    assertEquals(metadata.getAllColumnMetadata().size(), columns.size());
    for (String column : columns) {
      assertNotNull(metadata.getColumnMetadataFor(column), column);
    }
    Map<String, ColumnMetadata> visited = new LinkedHashMap<>();
    metadata.forEachColumn(visited::put);
    assertEquals(new ArrayList<>(visited.keySet()), columns);
    metadata.toJson(null);
    assertFalse(metadata.isColumnMetadataMapMaterialized(), "reading the columns must not build the map");
    assertEquals(SegmentMetadataImpl.getNumColumnMetadataMapMaterializations(), materializations);

    TreeMap<String, ColumnMetadata> map = metadata.getColumnMetadataMap();
    assertTrue(metadata.isColumnMetadataMapMaterialized());
    assertEquals(SegmentMetadataImpl.getNumColumnMetadataMapMaterializations(), materializations + 1);
    assertEquals(map, visited);
    assertEquals(new ArrayList<>(map.keySet()), columns);
    assertSame(metadata.getColumnMetadataMap(), map);
    assertEquals(SegmentMetadataImpl.getNumColumnMetadataMapMaterializations(), materializations + 1);
    assertNull(metadata.getColumnMetadataFor("noSuchColumn"));
  }

  /// getAllColumns() is a view of the metadata's own name array, so it must refuse every mutator rather than let a
  /// caller narrow a loaded segment's columns, and it must not reflect later column changes.
  @Test
  public void testGetAllColumnsIsAnUnmodifiableSnapshot()
      throws Exception {
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(_segmentDirectory);
    NavigableSet<String> columns = metadata.getAllColumns();
    String column = columns.stream().filter(c -> !c.equals(metadata.getTimeColumn())).findFirst().orElseThrow();
    assertThrows(UnsupportedOperationException.class, () -> columns.remove(column));
    assertThrows(UnsupportedOperationException.class, () -> columns.retainAll(Set.of(column)));

    metadata.removeColumn(column);
    assertTrue(columns.contains(column), "the earlier view stays the snapshot it was");
    assertFalse(metadata.getAllColumns().contains(column));
  }

  /// The loader registers the built-in virtual columns through addColumnMetadata(), which has to keep the arrays
  /// sorted and drop both derived views.
  @Test
  public void testAddColumnMetadataKeepsColumnsSortedAndDropsDerivedViews()
      throws Exception {
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(_segmentDirectory);
    assertNotNull(metadata.getColumnMetadataMap());
    assertNotNull(metadata.getSchema());
    int numColumns = metadata.getNumColumns();

    String column = "$aVirtualColumn";
    ColumnMetadata added =
        new EmptyColumnMetadata(new DimensionFieldSpec(column, FieldSpec.DataType.INT, true), null, null);
    metadata.addColumnMetadata(column, added);
    assertFalse(metadata.isColumnMetadataMapMaterialized());
    assertFalse(metadata.isSchemaMaterialized());
    assertEquals(metadata.getNumColumns(), numColumns + 1);
    assertSame(metadata.getColumnMetadataFor(column), added);
    assertEquals(new ArrayList<>(metadata.getAllColumns()), new ArrayList<>(metadata.getColumnMetadataMap().keySet()));
    assertEquals(metadata.getAllColumns().first(), column, "must be inserted in natural order, not appended");
    assertTrue(metadata.getSchema().hasColumn(column));

    // Re-registering replaces the column rather than duplicating it
    ColumnMetadata replacement =
        new EmptyColumnMetadata(new DimensionFieldSpec(column, FieldSpec.DataType.LONG, true), null, null);
    metadata.addColumnMetadata(column, replacement);
    assertEquals(metadata.getNumColumns(), numColumns + 1);
    assertSame(metadata.getColumnMetadataFor(column), replacement);
  }

  /// The column arrays are replaced as a whole, never written in place, so a collection handed out earlier stays the
  /// snapshot it is documented to be — through an insertion and through a replacement of a column already there —
  /// and every name stays paired with its own metadata.
  @Test
  public void testColumnMetadataViewIsASnapshot()
      throws Exception {
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(_segmentDirectory);
    int numColumns = metadata.getNumColumns();
    Collection<ColumnMetadata> snapshot = metadata.getAllColumnMetadata();
    String column = metadata.getAllColumns().first();
    ColumnMetadata original = metadata.getColumnMetadataFor(column);
    assertSame(new ArrayList<>(snapshot).get(0), original);

    metadata.addColumnMetadata(column,
        new EmptyColumnMetadata(new DimensionFieldSpec(column, FieldSpec.DataType.INT, true), null, null));
    metadata.addColumnMetadata("$aVirtualColumn",
        new EmptyColumnMetadata(new DimensionFieldSpec("$aVirtualColumn", FieldSpec.DataType.INT, true), null, null));
    assertEquals(snapshot.size(), numColumns);
    assertSame(new ArrayList<>(snapshot).get(0), original, "a replacement must not reach the earlier snapshot");
    assertNotSame(metadata.getColumnMetadataFor(column), original);

    assertEquals(metadata.getNumColumns(), numColumns + 1);
    assertEquals(metadata.getAllColumnMetadata().size(), numColumns + 1);
    metadata.forEachColumn((name, columnMetadata) -> assertEquals(columnMetadata.getColumnName(), name));
  }

  /// A CONSUMING segment holds no column metadata: its column names come from the explicit schema, the column
  /// metadata accessors are empty, and both mutators reject it rather than drop the schema it was given.
  @Test
  public void testConsumingSegmentHoldsNoColumnMetadata() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("consuming")
        .addSingleValueDimension("dim", FieldSpec.DataType.STRING)
        .addMetric("metric", FieldSpec.DataType.LONG)
        .build();
    SegmentMetadataImpl metadata =
        new SegmentMetadataImpl("testTable", "testTable__0__0__20240101T0000Z", schema, 123L);
    assertEquals(metadata.getAllColumns(), schema.getColumnNames());
    assertEquals(metadata.getNumColumns(), schema.size());
    assertTrue(metadata.getAllColumnMetadata().isEmpty());
    assertNull(metadata.getColumnMetadataMap());
    assertNull(metadata.getColumnMetadataFor("dim"));
    metadata.forEachColumn((column, columnMetadata) -> fail("no column metadata to visit, got: " + column));

    ColumnMetadata added =
        new EmptyColumnMetadata(new DimensionFieldSpec("added", FieldSpec.DataType.INT, true), null, null);
    assertThrows(IllegalStateException.class, () -> metadata.addColumnMetadata("added", added));
    assertThrows(IllegalStateException.class, () -> metadata.removeColumn("dim"));
    assertSame(metadata.getSchema(), schema, "the explicit schema survives a rejected mutation");
    assertEquals(metadata.getAllColumns(), schema.getColumnNames());
  }

  /// The metadata JSON is a public REST payload: it must list the columns in the same natural order the map view
  /// does, filter included.
  @Test
  public void testToJsonColumnOrderMatchesTheMapView()
      throws Exception {
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(_segmentDirectory);
    List<String> expected = new ArrayList<>(metadata.getColumnMetadataMap().keySet());
    List<String> actual = new ArrayList<>();
    metadata.toJson(null).get("columns").forEach(column -> actual.add(column.get("columnName").asText()));
    assertEquals(actual, expected);

    Set<String> filter = Set.of(expected.get(expected.size() - 1), expected.get(0));
    List<String> filtered = new ArrayList<>();
    metadata.toJson(filter).get("columns").forEach(column -> filtered.add(column.get("columnName").asText()));
    assertEquals(filtered, List.of(expected.get(0), expected.get(expected.size() - 1)));
  }

  /// removeColumn() drops the column from the column metadata and from any schema derived afterwards.
  @Test
  public void testRemoveColumnInvalidatesDerivedSchema()
      throws Exception {
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(_segmentDirectory);
    Schema before = metadata.getSchema();
    String column = metadata.getAllColumns().stream().filter(c -> !c.equals(metadata.getTimeColumn())).findFirst()
        .orElseThrow();
    assertTrue(before.hasColumn(column));

    metadata.removeColumn(column);
    assertFalse(metadata.isSchemaMaterialized());
    assertFalse(metadata.getAllColumns().contains(column));
    assertNull(metadata.getColumnMetadataFor(column));
    Schema after = metadata.getSchema();
    assertNotSame(after, before);
    assertFalse(after.hasColumn(column));
    assertEquals(after.size(), before.size() - 1);
    assertEquals(after.getColumnNames(), metadata.getAllColumns());
  }

  /// A CONSUMING segment is constructed with its schema, which is handed back as is (and named in the JSON) rather
  /// than derived: it has no column metadata to derive from.
  @Test
  public void testExplicitSchemaIsReturnedAsIs() {
    long materializations = SegmentMetadataImpl.getNumSchemaMaterializations();
    Schema schema = new Schema.SchemaBuilder().setSchemaName("consuming")
        .addSingleValueDimension("dim", FieldSpec.DataType.STRING)
        .addMetric("metric", FieldSpec.DataType.LONG)
        .build();
    SegmentMetadataImpl metadata =
        new SegmentMetadataImpl("testTable", "testTable__0__0__20240101T0000Z", schema, 123L);
    assertTrue(metadata.isSchemaMaterialized());
    assertSame(metadata.getSchema(), schema);
    assertEquals(metadata.getAllColumns(), schema.getColumnNames());
    assertEquals(metadata.toJson(null).get("schemaName").asText(), "consuming");
    assertEquals(SegmentMetadataImpl.getNumSchemaMaterializations(), materializations);
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
