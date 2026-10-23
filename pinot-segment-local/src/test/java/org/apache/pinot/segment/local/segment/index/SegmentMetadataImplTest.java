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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.converter.SegmentV1V2ToV3FormatConverter;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.metadata.MappedColumnMetadata;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.ColumnIndexUtils;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
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
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
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
    Assert.assertEquals(jsonMeta.get("crc").asLong(), metadata.getCrc());
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

  @Test
  public void testServingLoadFallsBackOrMappingIsDisabled()
      throws Exception {
    InstanceDataManagerConfig instanceConfig = mock(InstanceDataManagerConfig.class);
    PinotConfiguration config = new PinotConfiguration();
    when(instanceConfig.getConfig()).thenReturn(config);
    config.setProperty(IndexLoadingConfig.MAPPED_SEGMENT_METADATA_ENABLED, true);

    File sidecar = new File(_segmentDirectory, "segment_metadata.bin");
    assertTrue(sidecar.mkdir());
    Files.writeString(new File(sidecar, "blocker").toPath(), "keep directory non-empty");
    try {
      ImmutableSegment segment =
          ImmutableSegmentLoader.load(_segmentDirectory, new IndexLoadingConfig(instanceConfig, null, null), false);
      try {
        assertFalse(((SegmentMetadataImpl) segment.getSegmentMetadata()).isMapped());
        assertTrue(segment.getColumnNames().contains("daysSinceEpoch"));
      } finally {
        segment.destroy();
      }
    } finally {
      FileUtils.deleteQuietly(sidecar);
    }

    config.setProperty(IndexLoadingConfig.MAPPED_SEGMENT_METADATA_ENABLED, false);
    ImmutableSegment disabled =
        ImmutableSegmentLoader.load(_segmentDirectory, new IndexLoadingConfig(instanceConfig, null, null), false);
    try {
      assertFalse(((SegmentMetadataImpl) disabled.getSegmentMetadata()).isMapped());
      assertFalse(sidecar.exists());
    } finally {
      disabled.destroy();
    }

    ImmutableSegment reader = ImmutableSegmentLoader.load(_segmentDirectory, ReadMode.mmap);
    try {
      assertFalse(((SegmentMetadataImpl) reader.getSegmentMetadata()).isMapped());
      assertFalse(sidecar.exists());
    } finally {
      reader.destroy();
    }

    config.setProperty(IndexLoadingConfig.MAPPED_SEGMENT_METADATA_ENABLED, true);
    ImmutableSegment segment =
        ImmutableSegmentLoader.load(_segmentDirectory, new IndexLoadingConfig(instanceConfig, null, null), false);
    try {
      assertTrue(((SegmentMetadataImpl) segment.getSegmentMetadata()).isMapped());
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testMappedMetadataRoundTripAndRebuild()
      throws Exception {
    new SegmentV1V2ToV3FormatConverter().convert(_segmentDirectory);
    File properties = SegmentDirectoryPaths.findMetadataFile(_segmentDirectory);
    Files.writeString(properties.toPath(), "\ncustom.escaped = alpha\\,beta\n", StandardCharsets.UTF_8,
        StandardOpenOption.APPEND);
    SegmentMetadataImpl eager = new SegmentMetadataImpl(_segmentDirectory);
    long mappings = PinotDataBuffer.getMmapBufferCount();
    File sidecar = new File(_segmentDirectory, "segment_metadata.bin");
    try (SegmentMetadataImpl mapped = SegmentMetadataImpl.loadMapped(_segmentDirectory)) {
      assertTrue(mapped.isMapped());
      assertTrue(sidecar.isFile());
      assertEquals(mapped.getName(), eager.getName());
      assertEquals(mapped.getTotalDocs(), eager.getTotalDocs());
      assertEquals(mapped.getVersion(), eager.getVersion());
      assertEquals(mapped.getTimeColumn(), eager.getTimeColumn());
      assertEquals(mapped.getCustomMap(), eager.getCustomMap());
      assertEquals(mapped.getPhysicalColumnNames(), eager.getSchema().getPhysicalColumnNames());
      assertEquals(mapped.toJson(null), eager.toJson(null));
      for (String column : mapped.getPhysicalColumnNames()) {
        ColumnMetadata expected = eager.getColumnMetadataFor(column);
        ColumnMetadata actual = mapped.getColumnMetadataFor(column);
        assertTrue(actual instanceof MappedColumnMetadata);
        assertSame(mapped.getColumnMetadataFor(column), actual);
        assertEquals(actual.getFieldSpec(), expected.getFieldSpec(), column);
        assertEquals(actual.getCardinality(), expected.getCardinality(), column);
        assertEquals(actual.getMinValue(), expected.getMinValue(), column);
        assertEquals(actual.getMaxValue(), expected.getMaxValue(), column);
        assertEquals(actual.getNumIndexes(), expected.getNumIndexes(), column);
        assertEquals(((MappedColumnMetadata) actual).getIndexSizeMap(),
            ((ColumnMetadataImpl) expected).getIndexSizeMap(), column);
      }
    }
    assertEquals(PinotDataBuffer.getMmapBufferCount(), mappings);

    byte[] sidecarBytes = Files.readAllBytes(sidecar.toPath());
    assertEquals(ByteBuffer.wrap(sidecarBytes).order(ByteOrder.BIG_ENDIAN).getInt(40), sidecarCrc(sidecarBytes));

    // An invalid sidecar is regenerated from canonical metadata.properties.
    Files.write(sidecar.toPath(), new byte[]{1, 2, 3});
    try (SegmentMetadataImpl mapped = SegmentMetadataImpl.loadMapped(_segmentDirectory)) {
      assertEquals(mapped.getTotalDocs(), eager.getTotalDocs());
    }
    // A source-only update invalidates a valid sidecar as well.
    Files.writeString(properties.toPath(), "\ncustom.mappedRefresh = yes\n", StandardCharsets.UTF_8,
        StandardOpenOption.APPEND);
    try (SegmentMetadataImpl mapped = SegmentMetadataImpl.loadMapped(_segmentDirectory)) {
      assertEquals(mapped.getCustomMap().get("mappedRefresh"), "yes");
    }
    // A zero-sized local v3 index is still present; a root-only map does not supply eager or mapped index sizes.
    File indexMap = new File(new File(_segmentDirectory, "v3"), V1Constants.INDEX_MAP_FILE_NAME);
    PropertiesConfiguration indexConfig = CommonsConfigurationUtils.fromFile(indexMap);
    String sizeKey = CommonsConfigurationUtils.getKeys(indexConfig).stream()
        .filter(key -> ColumnIndexUtils.MAP_KEY_NAME_SIZE.equals(
            ColumnIndexUtils.parseIndexMapKeys(key, _segmentDirectory.getPath())[2])).findFirst().orElseThrow();
    String[] parsedKey = ColumnIndexUtils.parseIndexMapKeys(sizeKey, _segmentDirectory.getPath());
    indexConfig.setProperty(sizeKey, 0);
    CommonsConfigurationUtils.saveToFile(indexConfig, indexMap);
    try (SegmentMetadataImpl mapped = SegmentMetadataImpl.loadMapped(_segmentDirectory)) {
      ColumnMetadata zeroSizedColumn = mapped.getColumnMetadataFor(parsedKey[0]);
      assertEquals(zeroSizedColumn.getIndexSizeFor(IndexService.getInstance().get(parsedKey[1])), 0L);
    }
    File staging = new File(INDEX_DIR, "mapped-staging");
    try {
      FileUtils.forceMkdir(staging);
      FileUtils.copyFile(properties, new File(staging, properties.getName()));
      File creationMeta = SegmentDirectoryPaths.findCreationMetaFile(_segmentDirectory);
      FileUtils.copyFile(creationMeta, new File(staging, creationMeta.getName()));
      indexConfig.setProperty(sizeKey, 123);
      CommonsConfigurationUtils.saveToFile(indexConfig, new File(staging, V1Constants.INDEX_MAP_FILE_NAME));
      SegmentMetadataImpl stagedEager = new SegmentMetadataImpl(staging);
      try (SegmentMetadataImpl mapped = SegmentMetadataImpl.loadMapped(staging)) {
        assertEquals(stagedEager.getColumnMetadataFor(parsedKey[0]).getIndexSizeFor(
            IndexService.getInstance().get(parsedKey[1])), ColumnMetadata.UNAVAILABLE);
        assertEquals(mapped.getColumnMetadataFor(parsedKey[0]).getIndexSizeFor(
            IndexService.getInstance().get(parsedKey[1])),
            ColumnMetadata.UNAVAILABLE);
      }
    } finally {
      FileUtils.deleteQuietly(staging);
    }
    assertEquals(PinotDataBuffer.getMmapBufferCount(), mappings);
  }

  @Test
  public void testMappedMetadataRebuildsChecksumValidInvalidFieldSpec()
      throws Exception {
    new SegmentV1V2ToV3FormatConverter().convert(_segmentDirectory);
    File sidecar = new File(_segmentDirectory, "segment_metadata.bin");
    try (SegmentMetadataImpl ignored = SegmentMetadataImpl.loadMapped(_segmentDirectory)) {
      assertTrue(ignored.isMapped());
    }
    byte[] bytes = Files.readAllBytes(sidecar.toPath());
    ByteBuffer header = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
    int fieldSpecOffset = header.getInt(header.getInt(56) + 8);
    assertEquals(bytes[fieldSpecOffset], (byte) '{');
    bytes[fieldSpecOffset] = '!';
    header.putInt(40, sidecarCrc(bytes));
    Files.write(sidecar.toPath(), bytes);

    try (SegmentMetadataImpl mapped = SegmentMetadataImpl.loadMapped(_segmentDirectory)) {
      assertTrue(mapped.isMapped());
    }
    assertEquals(Files.readAllBytes(sidecar.toPath())[fieldSpecOffset], (byte) '{');
  }

  @Test
  public void testMappedLegacyTimeFieldSpecAndVersionRebuild()
      throws Exception {
    File segmentDir = Files.createTempDirectory("mapped-legacy-time").toFile();
    try {
      Files.writeString(new File(segmentDir, "metadata.properties").toPath(),
          "segment.name = legacyTime\n"
              + "segment.table.name = testTable\n"
              + "segment.total.docs = 0\n"
              + "segment.version = v1\n"
              + "segment.time.column.name = timestamp\n"
              + "segment.time.unit = MILLISECONDS\n"
              + "segment.complex.column.names = parent\n"
              + "column.timestamp.columnType = TIME\n"
              + "column.timestamp.dataType = LONG\n"
              + "column.parent.columnType = COMPLEX\n"
              + "column.parent.dataType = OPEN_STRUCT\n"
              + "column.parent.complexChildFieldNames = ts\n"
              + "column.parent$$ts.columnType = TIME\n"
              + "column.parent$$ts.dataType = LONG\n");
      SegmentMetadataImpl eager = new SegmentMetadataImpl(segmentDir);
      ComplexFieldSpec parent = (ComplexFieldSpec) eager.getColumnMetadataFor("parent").getFieldSpec();
      assertEquals(parent.getChildFieldSpec("ts").getFieldType(), FieldSpec.FieldType.TIME);
      File sidecar = new File(segmentDir, "segment_metadata.bin");
      for (int i = 0; i < 2; i++) {
        try (SegmentMetadataImpl mapped = SegmentMetadataImpl.loadMapped(segmentDir)) {
          assertEquals(mapped.getColumnMetadataFor("timestamp").getFieldSpec().getFieldType(),
              FieldSpec.FieldType.TIME);
          assertEquals(mapped.getSchema(), eager.getSchema());
        }
      }
      // A v3 sidecar with its original payload-only checksum must rebuild under the v4 format.
      byte[] version3 = Files.readAllBytes(sidecar.toPath());
      ByteBuffer version3Header = ByteBuffer.wrap(version3).order(ByteOrder.BIG_ENDIAN);
      version3Header.putInt(4, 3);
      CRC32C payloadCrc = new CRC32C();
      payloadCrc.update(version3, 64, version3.length - 64);
      version3Header.putInt(40, (int) payloadCrc.getValue());
      Files.write(sidecar.toPath(), version3);
      try (SegmentMetadataImpl mapped = SegmentMetadataImpl.loadMapped(segmentDir)) {
        assertEquals(mapped.getSchema(), eager.getSchema());
      }
      assertEquals(ByteBuffer.wrap(Files.readAllBytes(sidecar.toPath())).order(ByteOrder.BIG_ENDIAN).getInt(4), 4);
    } finally {
      FileUtils.deleteQuietly(segmentDir);
    }
  }

  private static int sidecarCrc(byte[] bytes) {
    byte[] withZeroedSlot = Arrays.copyOf(bytes, bytes.length);
    ByteBuffer.wrap(withZeroedSlot).order(ByteOrder.BIG_ENDIAN).putInt(40, 0);
    CRC32C crc = new CRC32C();
    crc.update(withZeroedSlot, 0, withZeroedSlot.length);
    return (int) crc.getValue();
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
    SegmentMetadataImpl reloaded = new SegmentMetadataImpl(_segmentDirectory);
    for (String column : second.getColumnMetadataMap().keySet()) {
      assertSame(reloaded.getColumnMetadataFor(column).getFieldSpec(),
          second.getColumnMetadataFor(column).getFieldSpec(), column);
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

  /// A COMPLEX parent retains its own child map, but its children and the materialized child columns are interned.
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
      try (SegmentMetadataImpl mapped = SegmentMetadataImpl.loadMapped(segmentDir)) {
        assertEquals(mapped.toJson(null), first.toJson(null));
        ColumnMetadata eagerParent = first.getColumnMetadataFor(parent);
        ColumnMetadata mappedParent = mapped.getColumnMetadataFor(parent);
        assertEquals(mappedParent.getFieldSpec(), eagerParent.getFieldSpec());
        assertEquals(mappedParent.getSparseKeys(), eagerParent.getSparseKeys());
        assertEquals(mappedParent.getSparseMultiValueKeys(), eagerParent.getSparseMultiValueKeys());
        assertEquals(mappedParent.getSparseKeyTypes(), eagerParent.getSparseKeyTypes());
        ColumnMetadata mappedChild = mapped.getColumnMetadataFor(child);
        assertEquals(mappedChild.getFieldSpec(), first.getColumnMetadataFor(child).getFieldSpec());
        assertEquals(mappedChild.isMaterializedChild(), first.getColumnMetadataFor(child).isMaterializedChild());
        assertEquals(mappedChild.getParentColumn(), first.getColumnMetadataFor(child).getParentColumn());
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
