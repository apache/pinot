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
package org.apache.pinot.segment.local.segment.index.column;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.json.JsonIndexType;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FieldConfig.Builder;
import org.apache.pinot.spi.config.table.FieldConfig.IndexType;
import org.apache.pinot.spi.config.table.JsonIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static java.util.List.of;
import static org.testng.Assert.assertNotNull;


public class PhysicalColumnIndexContainerTest {

  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), PhysicalColumnIndexContainerTest.class.getSimpleName());

  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String INT_COL = "intColumn";
  private static final String LONG_COL = "longColumn";
  private static final String FLOAT_COL = "floatColumn";
  private static final String DOUBLE_COL = "doubleColumn";
  private static final String STR_COL = "stringColumn";

  private static final Schema
      SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
      .addSingleValueDimension(LONG_COL, FieldSpec.DataType.LONG)
      .addSingleValueDimension(FLOAT_COL, FieldSpec.DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_COL, FieldSpec.DataType.DOUBLE)
      .addSingleValueDimension(STR_COL, FieldSpec.DataType.STRING)
      .build();

  private static final TableConfig TABLE_CONFIG;

  static {
    ObjectNode indexes = JsonUtils.newObjectNode();
    JsonIndexConfig config = new JsonIndexConfig();
    indexes.put(JsonIndexType.INDEX_DISPLAY_NAME, config.toJsonNode());

    TABLE_CONFIG =
        new TableConfigBuilder(TableType.OFFLINE)
            .setTableName(RAW_TABLE_NAME)
            .addFieldConfig(new Builder(STR_COL)
                .withIndexes(indexes)
                .build())
            .addFieldConfig(new Builder(INT_COL)
                .withIndexTypes(of(IndexType.RANGE))
                .build())
            .addFieldConfig(new Builder(LONG_COL)
                .withIndexTypes(of(IndexType.RANGE))
                .build())
            .addFieldConfig(new Builder(FLOAT_COL)
                .withIndexTypes(of(IndexType.RANGE, IndexType.SORTED))
                .build())
            .setRangeIndexColumns(List.of(LONG_COL, FLOAT_COL))
            .build();
  }

  @Test
  public void testCreateSegmentAndCheckColumnIndexes()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    StringBuilder sb = new StringBuilder();
    List<GenericRow> records = new ArrayList<>(10);
    for (int i = 0; i < 10; i++) {
      sb.append("{ \"value\": ").append(i).append(" }");

      GenericRow record = new GenericRow();
      record.putValue(INT_COL, i);
      record.putValue(LONG_COL, (long) i);
      record.putValue(FLOAT_COL, (float) i);
      record.putValue(DOUBLE_COL, (double) i);
      record.putValue(STR_COL, sb.toString());
      records.add(record);
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    ImmutableSegment segment = null;
    try {
      segment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
      assertNotNull(segment.getIndex(STR_COL, StandardIndexes.json()));
      assertNotNull(segment.getIndex(STR_COL, StandardIndexes.dictionary()));
      assertNotNull(segment.getIndex(STR_COL, StandardIndexes.forward()));

      assertNotNull(segment.getIndex(FLOAT_COL, StandardIndexes.dictionary()));
      assertNotNull(segment.getIndex(FLOAT_COL, StandardIndexes.forward()));
      assertNotNull(segment.getIndex(FLOAT_COL, StandardIndexes.range()));

      assertNotNull(segment.getIndex(DOUBLE_COL, StandardIndexes.dictionary()));
      assertNotNull(segment.getIndex(DOUBLE_COL, StandardIndexes.forward()));

      assertNotNull(segment.getIndex(LONG_COL, StandardIndexes.dictionary()));
      assertNotNull(segment.getIndex(LONG_COL, StandardIndexes.forward()));
      assertNotNull(segment.getIndex(LONG_COL, StandardIndexes.range()));
    } finally {
      if (segment != null) {
        segment.destroy();
      }
    }
  }
}
