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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests filtering of records during segment generation
 */
public class SegmentGenerationWithMinMaxTest {
  private static final String STRING_COLUMN = "col1";
  private static final String[] STRING_VALUES_WITH_COMMA_CHARACTER = {"A,,", ",B,", "C,Z,", "D,", "E,"};
  private static final String[] STRING_VALUES_WITH_WHITESPACE_CHARACTERS = {"A ", " B ", "  Z ", "  \r D", "E"};
  private static final String[] STRING_VALUES_VALID = {"A", "B", "C", "D", "E"};
  private static final String LONG_COLUMN = "col2";
  private static final long[] LONG_VALUES =
      {1588316400000L, 1588489200000L, 1588662000000L, 1588834800000L, 1589007600000L};

  private static final String SEGMENT_DIR_NAME =
      FileUtils.getTempDirectoryPath() + File.separator + "segmentMinMaxTest";
  private static final String SEGMENT_NAME = "testSegmentMinMax";

  private Schema _schema;
  private TableConfig _tableConfig;

  @BeforeClass
  public void setup() {
    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    _schema = new Schema.SchemaBuilder().addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING)
        .addMetric(LONG_COLUMN, FieldSpec.DataType.LONG).build();
  }

  @Test
  public void testMinMaxInMetadata()
      throws Exception {
    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
    File segmentDir = buildSegment(_tableConfig, _schema, STRING_VALUES_VALID);
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);
    Assert.assertEquals(metadata.getTotalDocs(), 5);
    Assert.assertFalse(metadata.getColumnMetadataFor("col1").isMinMaxValueInvalid());
    Assert.assertEquals(metadata.getColumnMetadataFor("col1").getMinValue(), "A");
    Assert.assertEquals(metadata.getColumnMetadataFor("col1").getMaxValue(), "E");

    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
    segmentDir = buildSegment(_tableConfig, _schema, STRING_VALUES_WITH_COMMA_CHARACTER);
    metadata = new SegmentMetadataImpl(segmentDir);
    Assert.assertEquals(metadata.getTotalDocs(), 5);
    Assert.assertFalse(metadata.getColumnMetadataFor("col1").isMinMaxValueInvalid());
    Assert.assertEquals(metadata.getColumnMetadataFor("col1").getMinValue(), ",B,");
    Assert.assertEquals(metadata.getColumnMetadataFor("col1").getMaxValue(), "E,");

    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
    segmentDir = buildSegment(_tableConfig, _schema, STRING_VALUES_WITH_WHITESPACE_CHARACTERS);
    metadata = new SegmentMetadataImpl(segmentDir);
    Assert.assertEquals(metadata.getTotalDocs(), 5);
    Assert.assertFalse(metadata.getColumnMetadataFor("col1").isMinMaxValueInvalid());
    Assert.assertEquals(metadata.getColumnMetadataFor("col1").getMinValue(), "  \r D");
    Assert.assertEquals(metadata.getColumnMetadataFor("col1").getMaxValue(), "E");
  }

  private File buildSegment(final TableConfig tableConfig, final Schema schema, String[] stringValues)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(SEGMENT_DIR_NAME);
    config.setSegmentName(SEGMENT_NAME);

    List<GenericRow> rows = new ArrayList<>(3);
    for (int i = 0; i < 5; i++) {
      GenericRow row = new GenericRow();
      row.putValue(STRING_COLUMN, stringValues[i]);
      row.putValue(LONG_COLUMN, LONG_VALUES[i]);
      rows.add(row);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    driver.getOutputDirectory().deleteOnExit();
    return driver.getOutputDirectory();
  }
}
