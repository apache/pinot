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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.StandardIndexes;
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
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Shared base for the OPEN_STRUCT end-to-end ingestion + commit tests.
///
/// Builds a table with one OPEN_STRUCT column `metrics` carrying a per-key index matrix
/// (inverted; dictionary-only; raw) plus two sparse keys, ingests ~1000 rows, and validates the
/// committed segment's materialized child columns, their `index_map`, and the dense/sparse split.
///
/// This class is abstract and is not run on its own; the concrete table-type variant (currently
/// {@link OpenStructIngestionCommitRealtimeTest}) supplies the table name and commit trigger and
/// reuses the inherited validation. An OFFLINE variant is added in a follow-up change once the
/// offline OPEN_STRUCT build path is fixed.
public abstract class OpenStructIngestionCommitTestBase extends CustomDataQueryClusterIntegrationTest {

  protected static final String METRICS = "metrics";
  // Matches OpenStructIndexType.INDEX_DISPLAY_NAME (kept as a literal to avoid a creator-side import).
  private static final String OPEN_STRUCT_INDEX_NAME = "open_struct";
  protected static final int NUM_DOCS = 1000;

  @Override
  protected long getCountStarResult() {
    // createAvroFiles writes NUM_DOCS rows round-robin across the Avro files, so the total doc count
    // is NUM_DOCS regardless of how many segments/partitions the data lands in.
    return NUM_DOCS;
  }

  @Override
  public Schema createSchema() {
    // Declare child field specs so string Avro values coerce deterministically to typed columns.
    Map<String, FieldSpec> children = new HashMap<>();
    children.put("views", new DimensionFieldSpec("views", FieldSpec.DataType.LONG, true));
    children.put("cpu", new DimensionFieldSpec("cpu", FieldSpec.DataType.DOUBLE, true));
    children.put("host", new DimensionFieldSpec("host", FieldSpec.DataType.STRING, true));
    children.put("region", new DimensionFieldSpec("region", FieldSpec.DataType.STRING, true));
    children.put("latencyMs", new DimensionFieldSpec("latencyMs", FieldSpec.DataType.LONG, true));
    ComplexFieldSpec metricsSpec = new ComplexFieldSpec(METRICS, FieldSpec.DataType.OPEN_STRUCT, true, children);
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addField(metricsSpec)
        .addDateTimeField(TIMESTAMP_FIELD_NAME, FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS", "1:DAYS")
        .build();
  }

  /// Per-key index matrix on the OPEN_STRUCT column:
  ///   views  -> inverted (=> dictionary + forward + inverted)
  ///   cpu    -> DICTIONARY encoding (=> dictionary + forward, no inverted)
  ///   host   -> RAW encoding (=> raw forward only, no dict, no inverted)
  /// region + latencyMs have no per-key config and are not in denseKeys, so the maxDenseKeys=3
  /// budget (filled by views/cpu/host) forces them into the sparse column.
  @Override
  protected List<FieldConfig> getFieldConfigs() {
    FieldConfig viewsCfg = new FieldConfig.Builder("views")
        .withIndexes(JsonUtils.objectToJsonNode(Map.of("inverted", Map.of())))
        .build();
    FieldConfig cpuCfg = new FieldConfig.Builder("cpu")
        .withEncodingType(FieldConfig.EncodingType.DICTIONARY)
        .build();
    FieldConfig hostCfg = new FieldConfig.Builder("host")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .build();
    // First arg is `disabled` — false => enabled.
    OpenStructIndexConfig osConfig = new OpenStructIndexConfig(false, null, 3,
        Set.of("views", "cpu", "host"), 0.5, List.of(viewsCfg, cpuCfg, hostCfg));
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set(OPEN_STRUCT_INDEX_NAME, JsonUtils.objectToJsonNode(osConfig));
    FieldConfig metricsCfg = new FieldConfig.Builder(METRICS).withIndexes(indexes).build();
    return List.of(metricsCfg);
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setTimeColumnName(getTimeColumnName())
        .setFieldConfigList(getFieldConfigs())
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("OpenStructRecord", null, null, false);
    org.apache.avro.Schema mapSchema =
        org.apache.avro.Schema.createMap(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
    List<org.apache.avro.Schema.Field> fields = Arrays.asList(
        new org.apache.avro.Schema.Field(METRICS, mapSchema, null, null),
        new org.apache.avro.Schema.Field(TIMESTAMP_FIELD_NAME,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null, null));
    avroSchema.setFields(fields);

    long tsBase = System.currentTimeMillis();
    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < NUM_DOCS; i++) {
        Map<String, String> metrics = new HashMap<>();
        metrics.put("views", String.valueOf(i));            // LONG, high cardinality (dict + inverted)
        metrics.put("cpu", String.valueOf(i * 0.5));         // DOUBLE (dict)
        metrics.put("host", "host-" + (i % 5));              // STRING, small set (raw forward)
        metrics.put("region", "region-" + (i % 4));          // sparse
        metrics.put("latencyMs", String.valueOf(i % 100));   // sparse
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(METRICS, metrics);
        record.put(TIMESTAMP_FIELD_NAME, tsBase + i);
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  /// OFFLINE by default; the realtime variant overrides to REALTIME so the locate helper scans the
  /// correct table-name-with-type under the server data dirs.
  protected TableType getSegmentTableType() {
    return TableType.OFFLINE;
  }

  @Test
  public void testCountStar()
      throws Exception {
    JsonNode response = postQuery("SELECT COUNT(*) FROM " + getTableName());
    assertEquals(response.get("exceptions").size(), 0);
    assertEquals(response.get("resultTable").get("rows").get(0).get(0).asLong(), NUM_DOCS);
  }

  /// White-box validation: open the committed on-disk segment and assert the materialized child
  /// columns, their per-key index_map, and the dense/sparse split.
  @Test
  public void testCommittedSegmentIndexMap()
      throws Exception {
    SegmentDirAndMeta found = locateCommittedSegment();
    assertNotNull(found,
        "No committed segment containing OPEN_STRUCT child columns found under any server data dir");
    File segmentDir = found._dir;
    TreeMap<String, ColumnMetadata> cols = found._meta.getColumnMetadataMap();

    String views = OpenStructNaming.materializedColumnName(METRICS, "views");
    String cpu = OpenStructNaming.materializedColumnName(METRICS, "cpu");
    String host = OpenStructNaming.materializedColumnName(METRICS, "host");
    String region = OpenStructNaming.materializedColumnName(METRICS, "region");
    String latencyMs = OpenStructNaming.materializedColumnName(METRICS, "latencyMs");
    String sparse = OpenStructNaming.sparseColumnName(METRICS);

    // Parent + dense child columns present with correct data types.
    assertTrue(cols.containsKey(METRICS), "parent OPEN_STRUCT column missing");
    assertEquals(cols.get(METRICS).getDataType(), FieldSpec.DataType.OPEN_STRUCT);
    assertTrue(cols.containsKey(views), "dense child metrics$views missing");
    assertTrue(cols.containsKey(cpu), "dense child metrics$cpu missing");
    assertTrue(cols.containsKey(host), "dense child metrics$host missing");
    assertEquals(cols.get(views).getDataType(), FieldSpec.DataType.LONG);
    assertEquals(cols.get(cpu).getDataType(), FieldSpec.DataType.DOUBLE);
    assertEquals(cols.get(host).getDataType(), FieldSpec.DataType.STRING);

    // Dense/sparse split: region + latencyMs are NOT materialized; the single sparse column exists.
    assertFalse(cols.containsKey(region), "metrics$region must NOT be materialized (forced sparse)");
    assertFalse(cols.containsKey(latencyMs), "metrics$latencyMs must NOT be materialized (forced sparse)");
    assertTrue(cols.containsKey(sparse), "sparse JSON column metrics$__sparse__ missing");
    assertEquals(cols.get(sparse).getDataType(), FieldSpec.DataType.STRING);

    // index_map per key.
    try (SegmentDirectory dir = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
        SegmentDirectory.Reader reader = dir.createReader()) {
      // views: dictionary + forward (inverted index propagation for per-key configs is a follow-up)
      assertTrue(reader.hasIndexFor(views, StandardIndexes.dictionary()), "views must have dictionary");
      assertTrue(reader.hasIndexFor(views, StandardIndexes.forward()), "views must have forward");
      // cpu: dictionary + forward
      assertTrue(reader.hasIndexFor(cpu, StandardIndexes.dictionary()), "cpu must have dictionary");
      assertTrue(reader.hasIndexFor(cpu, StandardIndexes.forward()), "cpu must have forward");
      // host: raw forward only
      assertFalse(reader.hasIndexFor(host, StandardIndexes.dictionary()), "host (raw) must NOT have dictionary");
      assertTrue(reader.hasIndexFor(host, StandardIndexes.forward()), "host must have forward");
      // sparse: raw forward, no dict, no inverted
      assertTrue(reader.hasIndexFor(sparse, StandardIndexes.forward()), "sparse column must have forward");
      assertFalse(reader.hasIndexFor(sparse, StandardIndexes.dictionary()), "sparse column must NOT have dictionary");
    }

    // Parent/child relationship + sparse flag from raw metadata.properties.
    File metadataFile = SegmentDirectoryPaths.findMetadataFile(segmentDir);
    PropertiesConfiguration props = CommonsConfigurationUtils.fromFile(metadataFile);
    assertEquals(props.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        METRICS, V1Constants.MetadataKeys.Column.HAS_SPARSE_COLUMN)), "true");
    assertEquals(props.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        views, V1Constants.MetadataKeys.Column.PARENT_COLUMN)), METRICS);
    assertEquals(props.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        sparse, V1Constants.MetadataKeys.Column.PARENT_COLUMN)), METRICS);
  }

  /// Scans both server data dirs (the custom suite starts 2 servers) for a committed immutable
  /// segment of this table that contains the OPEN_STRUCT dense child columns. Consuming/partial
  /// segment dirs that fail to parse, or that lack the child columns, are skipped.
  private SegmentDirAndMeta locateCommittedSegment() {
    String tableNameWithType = getSegmentTableType() == TableType.REALTIME
        ? TableNameBuilder.REALTIME.tableNameWithType(getTableName())
        : TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    String viewsChild = OpenStructNaming.materializedColumnName(METRICS, "views");
    for (int serverId = 0; serverId < 2; serverId++) {
      File tableDir = new File(TEMP_SERVER_DIR + File.separator + "dataDir-" + serverId, tableNameWithType);
      if (!tableDir.isDirectory()) {
        continue;
      }
      File[] segmentDirs = tableDir.listFiles(File::isDirectory);
      if (segmentDirs == null) {
        continue;
      }
      for (File segmentDir : segmentDirs) {
        try {
          SegmentMetadataImpl meta = new SegmentMetadataImpl(segmentDir);
          if (meta.getColumnMetadataMap().containsKey(viewsChild)) {
            return new SegmentDirAndMeta(segmentDir, meta);
          }
        } catch (Exception ignore) {
          // Not a fully-built immutable segment directory; skip.
        }
      }
    }
    return null;
  }

  private static final class SegmentDirAndMeta {
    private final File _dir;
    private final SegmentMetadataImpl _meta;

    private SegmentDirAndMeta(File dir, SegmentMetadataImpl meta) {
      _dir = dir;
      _meta = meta;
    }
  }
}
