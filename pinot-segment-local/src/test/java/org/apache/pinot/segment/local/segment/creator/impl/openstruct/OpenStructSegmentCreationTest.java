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
package org.apache.pinot.segment.local.segment.creator.impl.openstruct;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
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
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Drives [SegmentIndexCreationDriverImpl] over OPEN_STRUCT data to verify the full offline
/// segment-build pipeline (transform → stats → creator → index) without a cluster.
public class OpenStructSegmentCreationTest {

  private static final String METRICS = "metrics";
  private static final File TMP_DIR =
      new File(FileUtils.getTempDirectory(), OpenStructSegmentCreationTest.class.getName());

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(TMP_DIR);
  }

  @Test
  public void testOfflineSegmentBuildWithOpenStruct()
      throws Exception {
    Map<String, FieldSpec> children = new HashMap<>();
    children.put("views", new DimensionFieldSpec("views", FieldSpec.DataType.LONG, true));
    children.put("cpu", new DimensionFieldSpec("cpu", FieldSpec.DataType.DOUBLE, true));
    children.put("host", new DimensionFieldSpec("host", FieldSpec.DataType.STRING, true));

    Schema schema = new Schema.SchemaBuilder().setSchemaName("testOpenStruct")
        .addField(new ComplexFieldSpec(METRICS, FieldSpec.DataType.OPEN_STRUCT, true, children))
        .addSingleValueDimension("dim", FieldSpec.DataType.STRING)
        .build();

    OpenStructIndexConfig osConfig =
        new OpenStructIndexConfig(false, null, 3, Set.of("views", "cpu", "host"), 0.5, List.of());

    com.fasterxml.jackson.databind.node.ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("open_struct", JsonUtils.objectToJsonNode(osConfig));
    FieldConfig metricsCfg = new FieldConfig.Builder(METRICS).withIndexes(indexes).build();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testOpenStruct")
        .setFieldConfigList(List.of(metricsCfg)).build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(TMP_DIR.getAbsolutePath());
    config.setSegmentName("testSegment");

    int numRows = 100;
    List<GenericRow> rows = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      GenericRow row = new GenericRow();
      Map<String, Object> metrics = new HashMap<>();
      metrics.put("views", (long) i);
      metrics.put("cpu", i * 0.5);
      metrics.put("host", "host-" + (i % 5));
      metrics.put("region", "region-" + (i % 4));
      row.putValue(METRICS, metrics);
      row.putValue("dim", "val-" + i);
      rows.add(row);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    File segmentDir = driver.getOutputDirectory();
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);

    // Parent column present
    ColumnMetadata parentMeta = metadata.getColumnMetadataFor(METRICS);
    assertNotNull(parentMeta, "parent OPEN_STRUCT column missing");
    assertEquals(parentMeta.getDataType(), FieldSpec.DataType.OPEN_STRUCT);

    // Dense child columns present with correct types
    String views = OpenStructNaming.materializedColumnName(METRICS, "views");
    String cpu = OpenStructNaming.materializedColumnName(METRICS, "cpu");
    String host = OpenStructNaming.materializedColumnName(METRICS, "host");
    String sparse = OpenStructNaming.sparseColumnName(METRICS);

    assertNotNull(metadata.getColumnMetadataFor(views), "dense child views missing");
    assertNotNull(metadata.getColumnMetadataFor(cpu), "dense child cpu missing");
    assertNotNull(metadata.getColumnMetadataFor(host), "dense child host missing");
    assertEquals(metadata.getColumnMetadataFor(views).getDataType(), FieldSpec.DataType.LONG);
    assertEquals(metadata.getColumnMetadataFor(cpu).getDataType(), FieldSpec.DataType.DOUBLE);
    assertEquals(metadata.getColumnMetadataFor(host).getDataType(), FieldSpec.DataType.STRING);

    // Sparse column present (region is not in denseKeys and budget is full)
    assertNotNull(metadata.getColumnMetadataFor(sparse), "sparse column missing");

    // region should NOT be materialized (forced sparse)
    String region = OpenStructNaming.materializedColumnName(METRICS, "region");
    assertFalse(metadata.getColumnMetadataMap().containsKey(region), "region must be sparse, not materialized");

    // Verify index_map: dense children have forward index, parent does NOT
    try (SegmentDirectory dir = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
        SegmentDirectory.Reader reader = dir.createReader()) {
      assertTrue(reader.hasIndexFor(views, StandardIndexes.forward()), "views must have forward");
      assertTrue(reader.hasIndexFor(cpu, StandardIndexes.forward()), "cpu must have forward");
      assertTrue(reader.hasIndexFor(host, StandardIndexes.forward()), "host must have forward");
      assertTrue(reader.hasIndexFor(sparse, StandardIndexes.forward()), "sparse must have forward");
      assertFalse(reader.hasIndexFor(METRICS, StandardIndexes.forward()), "parent must NOT have forward");
    }

    assertEquals(metadata.getTotalDocs(), numRows);
  }
}
