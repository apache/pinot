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
package org.apache.pinot.core.segment.index.creator;

import com.google.common.collect.Lists;
import java.io.File;
import java.util.Collections;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.data.readers.PinotSegmentRecordReader;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.core.segment.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests segment generation for empty files
 */
public class SegmentGenerationWithNoRecordsTest {
  private static final String STRING_COLUMN1 = "string_col1";
  private static final String STRING_COLUMN2 = "string_col2";
  private static final String STRING_COLUMN3 = "string_col3";
  private static final String STRING_COLUMN4 = "string_col4";
  private static final String LONG_COLUMN1 = "long_col1";
  private static final String LONG_COLUMN2 = "long_col2";
  private static final String LONG_COLUMN3 = "long_col3";
  private static final String LONG_COLUMN4 = "long_col4";
  private static final String MV_INT_COLUMN = "mv_col";
  private static final String DATE_TIME_COLUMN = "date_time_col";
  private static final String SEGMENT_DIR_NAME =
      FileUtils.getTempDirectoryPath() + File.separator + "segmentNoRecordsTest";
  private static final String SEGMENT_NAME = "testSegment";

  private Schema _schema;
  private TableConfig _tableConfig;

  @BeforeClass
  public void setup() {
    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setTimeColumnName(DATE_TIME_COLUMN)
        .setInvertedIndexColumns(Lists.newArrayList(STRING_COLUMN1)).setSortedColumn(LONG_COLUMN1)
        .setRangeIndexColumns(Lists.newArrayList(STRING_COLUMN2))
        .setNoDictionaryColumns(Lists.newArrayList(LONG_COLUMN2)).setVarLengthDictionaryColumns(Lists.newArrayList(STRING_COLUMN3))
        .setOnHeapDictionaryColumns(Lists.newArrayList(LONG_COLUMN3)).build();
    _schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(STRING_COLUMN1, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN2, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN3, FieldSpec.DataType.STRING)
        .addSingleValueDimension(STRING_COLUMN4, FieldSpec.DataType.STRING)
        .addSingleValueDimension(LONG_COLUMN1, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN2, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LONG_COLUMN3, FieldSpec.DataType.LONG)
        .addMultiValueDimension(MV_INT_COLUMN, FieldSpec.DataType.INT)
        .addMetric(LONG_COLUMN4, FieldSpec.DataType.LONG)
        .addDateTime(DATE_TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
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
    Assert.assertEquals(metadata.getTotalDocs(), 0);
    Assert.assertTrue(metadata.getAllColumns().containsAll(_schema.getColumnNames()));
    PinotSegmentRecordReader segmentRecordReader = new PinotSegmentRecordReader(segmentDir);
    Assert.assertFalse(segmentRecordReader.hasNext());
  }

  private File buildSegment(final TableConfig tableConfig, final Schema schema)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(SEGMENT_DIR_NAME);
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(Collections.emptyList()));
    driver.build();
    driver.getOutputDirectory().deleteOnExit();
    return driver.getOutputDirectory();
  }
}
