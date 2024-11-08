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
package org.apache.pinot.queries;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Queries test for DATE and TIME data type.
 */
public class DateAndTimeQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "DateAndTimeQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final long BASE_DAYS = LocalDate.parse("2024-11-11").toEpochDay();
  private static final long BASE_MILLS_OF_DAY = LocalTime.parse("11:11:11.111").toNanoOfDay() / 1000000L;

  private static final int NUM_RECORDS = 1000;

  private static final String DATE_COLUMN = "dateColumn";
  private static final String TIME_COLUMN = "timeColumn";
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addSingleValueDimension(DATE_COLUMN, DataType.DATE)
          .addSingleValueDimension(TIME_COLUMN, DataType.TIME).build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      long days = BASE_DAYS + i;
      long mills = BASE_MILLS_OF_DAY + i;
      // Insert data in 3 different formats
      if (i % 3 == 0) {
        record.putValue(DATE_COLUMN, days);
        record.putValue(TIME_COLUMN, mills);
      } else if (i % 3 == 1) {
        record.putValue(DATE_COLUMN, LocalDate.ofEpochDay(days));
        record.putValue(TIME_COLUMN, LocalTime.ofNanoOfDay(mills * 1000000L));
      } else {
        record.putValue(DATE_COLUMN, LocalDate.ofEpochDay(days).toString());
        record.putValue(TIME_COLUMN, LocalTime.ofNanoOfDay(mills * 1000000L).toString());
      }
      records.add(record);
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @Test
  public void testQueries() {
    {
      String query = "SELECT * FROM testTable";
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{DATE_COLUMN, TIME_COLUMN},
              new ColumnDataType[]{ColumnDataType.DATE, ColumnDataType.TIME}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      for (int i = 0; i < 10; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 2);
        assertEquals(row[0], LocalDate.ofEpochDay(BASE_DAYS + i).toString());
        assertEquals(row[1], LocalTime.ofNanoOfDay((BASE_MILLS_OF_DAY + i) * 1000000L).toString());
      }
    }
    {
      // TODO: if use `ORDER BY dateColumn, timeColumn`, the test will fail
      String query = "SELECT * FROM testTable ORDER BY dateColumn DESC LIMIT 40";
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{DATE_COLUMN, TIME_COLUMN},
              new ColumnDataType[]{ColumnDataType.DATE, ColumnDataType.TIME}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 40);
      for (int i = 0; i < 10; i++) {
        String expectedDate = LocalDate.ofEpochDay(BASE_DAYS + NUM_RECORDS - 1 - i).toString();
        String expectedTime = LocalTime.ofNanoOfDay((BASE_MILLS_OF_DAY + NUM_RECORDS - 1 - i) * 1000000L).toString();
        for (int j = 0; j < 4; j++) {
          Object[] row = rows.get(i * 4 + j);
          assertEquals(row.length, 2);
          assertEquals(row[0], expectedDate);
          assertEquals(row[1], expectedTime);
        }
      }
    }
    {
      String query = "SELECT DISTINCT dateColumn, timeColumn FROM testTable ORDER BY dateColumn";
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{DATE_COLUMN, TIME_COLUMN},
              new ColumnDataType[]{ColumnDataType.DATE, ColumnDataType.TIME}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      for (int i = 0; i < 10; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 2);
        assertEquals(row[0], LocalDate.ofEpochDay(BASE_DAYS + i).toString());
        assertEquals(row[1], LocalTime.ofNanoOfDay((BASE_MILLS_OF_DAY + i) * 1000000L).toString());
      }
    }
    {
      String query =
          "SELECT COUNT(*) AS count, dateColumn, timeColumn FROM testTable GROUP BY dateColumn, timeColumn "
              + "ORDER BY timeColumn DESC";
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{"count", DATE_COLUMN, TIME_COLUMN},
          new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.DATE, ColumnDataType.TIME}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      for (int i = 0; i < 10; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 3);
        assertEquals(row[0], 4L);
        assertEquals(row[1], LocalDate.ofEpochDay(BASE_DAYS + NUM_RECORDS - i - 1).toString());
        assertEquals(row[2], LocalTime.ofNanoOfDay((BASE_MILLS_OF_DAY + NUM_RECORDS - i - 1) * 1000000L).toString());
      }
    }
  }

  @AfterClass
  public void tearDown()
          throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
