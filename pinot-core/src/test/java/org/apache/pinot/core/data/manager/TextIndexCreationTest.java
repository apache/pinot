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
package org.apache.pinot.core.data.manager;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.data.manager.offline.ImmutableSegmentDataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.utils.TableConfigUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class TextIndexCreationTest {

  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "TextIndexCreationTest");
  private static final String TABLE_NAME = "mytable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(TABLE_NAME);

  @Test
  public void testSameColumnSingleAndMultiColTextIndexCreationFails()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .addSingleValueDimension("txt", FieldSpec.DataType.STRING)
        .addDateTime("ts", FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .build();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(schema.getSchemaName())
        .addFieldConfig(new FieldConfig.Builder("txt").withIndexTypes(List.of(FieldConfig.IndexType.TEXT)).build())
        .setMultiColumnTextIndexConfig(new MultiColumnTextIndexConfig(List.of("txt")))
        .build();
    try {
      createImmutableSegment(tableConfig, schema, generateRows());
      Assert.fail();
    } catch (UnsupportedOperationException uoe) {
      Assert.assertEquals(uoe.getMessage(), "Cannot create both single and multi-column TEXT index on column: txt");
    }
  }

  protected static List<GenericRow> generateRows() {
    ArrayList<GenericRow> rows = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      GenericRow row = new GenericRow();
      row.putValue("txt", "test");
      row.putValue("ts", 365L * 24L * 60L * 100L);
      rows.add(row);
    }

    return rows;
  }

  private static void createImmutableSegment(TableConfig tableConfig, Schema schema,
      List<GenericRow> rows)
      throws Exception {
    // validate here to get better error messages (segment creation doesn't check everything  )
    TableConfigUtils.validate(tableConfig, schema);
    ImmutableSegmentDataManager segmentDataManager = mock(ImmutableSegmentDataManager.class);
    when(segmentDataManager.getSegmentName()).thenReturn("segmentName");
    File indexDir = createSegment(tableConfig, schema, "segmentName", rows);

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, schema);
    ImmutableSegment segment = null;
    try {
      segment = ImmutableSegmentLoader.load(indexDir, indexLoadingConfig);
    } finally {
      if (segment != null) {
        segment.destroy();
      }
    }
  }

  private static File createSegment(TableConfig tableConfig, Schema schema, String segmentName, List<GenericRow> rows)
      throws Exception {
    // load each segment in separate directory
    File dataDir = new File(TEMP_DIR, OFFLINE_TABLE_NAME + "_" + schema.getSchemaName());
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(dataDir.getAbsolutePath());
    config.setSegmentName(segmentName);
    config.setSegmentVersion(SegmentVersion.v3);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return new File(dataDir, segmentName);
  }
}
