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
import com.google.common.collect.Sets;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests many to one flattening based on presence of MULTIPLE_RECORDS_KEY in GenericRow
 */
public class SegmentGenerationWithMultipleRecordsTest {
  private static final String SUB_COLUMN_1 = "sub1";
  private static final String SUB_COLUMN_2 = "sub2";
  private static final String SEGMENT_DIR_NAME =
      System.getProperty("java.io.tmpdir") + File.separator + "segmentMultipleRecordsTest";
  private static final String SEGMENT_NAME = "testSegment";

  private Schema _schema;
  private TableConfig _tableConfig;

  @BeforeClass
  public void setup() {
    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("test").build();
    _schema = new Schema.SchemaBuilder().addSingleValueDimension(SUB_COLUMN_1, FieldSpec.DataType.STRING)
        .addMetric(SUB_COLUMN_2, FieldSpec.DataType.LONG).build();
  }

  @BeforeMethod
  public void reset() {
    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
  }

  @Test
  public void testNumDocs()
      throws Exception {
    File segmentDir = buildSegment(_tableConfig, _schema);
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);
    Assert.assertEquals(metadata.getTotalDocs(), 6);
    Assert.assertTrue(metadata.getAllColumns().containsAll(Sets.newHashSet(SUB_COLUMN_1, SUB_COLUMN_2)));
  }

  private File buildSegment(final TableConfig tableConfig, final Schema schema)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(SEGMENT_DIR_NAME);
    config.setSegmentName(SEGMENT_NAME);

    List<GenericRow> rows = new ArrayList<>(3);

    GenericRow genericRow1 = new GenericRow();
    genericRow1.putValue(GenericRow.MULTIPLE_RECORDS_KEY,
        Lists.newArrayList(getRandomArrayElement(), getRandomArrayElement(), getRandomArrayElement()));
    rows.add(genericRow1);
    GenericRow genericRow2 = new GenericRow();
    genericRow2.putValue(GenericRow.MULTIPLE_RECORDS_KEY, Lists.newArrayList(getRandomArrayElement()));
    rows.add(genericRow2);
    GenericRow genericRow3 = new GenericRow();
    genericRow3.putValue(GenericRow.MULTIPLE_RECORDS_KEY,
        Lists.newArrayList(getRandomArrayElement(), getRandomArrayElement()));
    rows.add(genericRow3);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    driver.getOutputDirectory().deleteOnExit();
    return driver.getOutputDirectory();
  }

  private GenericRow getRandomArrayElement() {
    GenericRow element = new GenericRow();
    element.putValue(SUB_COLUMN_1, RandomStringUtils.randomAlphabetic(4));
    element.putValue(SUB_COLUMN_2, RandomUtils.nextLong());
    return element;
  }
}
