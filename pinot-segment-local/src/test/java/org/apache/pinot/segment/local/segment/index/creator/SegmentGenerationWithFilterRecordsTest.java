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
package org.apache.pinot.segment.local.segment.index.creator;

import com.google.common.collect.Lists;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.local.segment.store.SegmentDirectory;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests filtering of records during segment generation
 */
public class SegmentGenerationWithFilterRecordsTest {
  private static final String STRING_COLUMN = "col1";
  private static final String[] STRING_VALUES = {"A", "B", "C", "D", "E"};
  private static final String LONG_COLUMN = "col2";
  private static final long[] LONG_VALUES =
      {1588316400000L, 1588489200000L, 1588662000000L, 1588834800000L, 1589007600000L};
  private static final String MV_INT_COLUMN = "col3";
  private static final ArrayList[] MV_INT_VALUES =
      {Lists.newArrayList(1, 2, 3), Lists.newArrayList(4), Lists.newArrayList(5, 1), Lists.newArrayList(
          2), Lists.newArrayList(3, 4, 5)};
  private static final String SEGMENT_DIR_NAME =
      FileUtils.getTempDirectoryPath() + File.separator + "segmentFilterRecordsTest";
  private static final String SEGMENT_NAME = "testSegment";

  private Schema _schema;
  private TableConfig _tableConfig;

  @BeforeClass
  public void setup() {
    String filterFunction =
        "Groovy({((col2 < 1589007600000L) &&  (col3.max() < 4)) || col1 == \"B\"}, col1, col2, col3)";
    IngestionConfig ingestionConfig = new IngestionConfig(null, null, new FilterConfig(filterFunction), null);
    _tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setIngestionConfig(ingestionConfig).build();
    _schema = new Schema.SchemaBuilder().addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING)
        .addMetric(LONG_COLUMN, FieldSpec.DataType.LONG).addMultiValueDimension(MV_INT_COLUMN, FieldSpec.DataType.INT)
        .build();
  }

  @BeforeMethod
  public void reset() {
    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
  }

  @Test
  public void testNumDocs()
      throws Exception {
    File segmentDir = buildSegment(_tableConfig, _schema);
    SegmentMetadataImpl metadata = SegmentDirectory.loadSegmentMetadata(segmentDir);
    Assert.assertEquals(metadata.getTotalDocs(), 2);
    PinotSegmentRecordReader segmentRecordReader = new PinotSegmentRecordReader(segmentDir);
    GenericRow next = segmentRecordReader.next();
    Assert.assertEquals(next.getValue(STRING_COLUMN), "C");
    next = segmentRecordReader.next();
    Assert.assertEquals(next.getValue(STRING_COLUMN), "E");
  }

  private File buildSegment(final TableConfig tableConfig, final Schema schema)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(SEGMENT_DIR_NAME);
    config.setSegmentName(SEGMENT_NAME);

    List<GenericRow> rows = new ArrayList<>(3);
    for (int i = 0; i < 5; i++) {
      GenericRow row = new GenericRow();
      row.putValue(STRING_COLUMN, STRING_VALUES[i]);
      row.putValue(LONG_COLUMN, LONG_VALUES[i]);
      row.putValue(MV_INT_COLUMN, MV_INT_VALUES[i]);
      rows.add(row);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    driver.getOutputDirectory().deleteOnExit();
    return driver.getOutputDirectory();
  }
}
