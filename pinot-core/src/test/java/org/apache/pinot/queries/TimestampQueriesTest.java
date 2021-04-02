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
import java.sql.Timestamp;
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
 * Queries test for TIMESTAMP data type.
 */
public class TimestampQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "TimestampQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final long BASE_TIMESTAMP = Timestamp.valueOf("2021-01-01 00:00:00").getTime();

  private static final int NUM_RECORDS = 1000;

  private static final String TIMESTAMP_COLUMN = "timestampColumn";
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addSingleValueDimension(TIMESTAMP_COLUMN, DataType.TIMESTAMP).build();
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
      long timestamp = BASE_TIMESTAMP + i;
      // Insert data in 3 different formats
      if (i % 3 == 0) {
        record.putValue(TIMESTAMP_COLUMN, timestamp);
      } else if (i % 3 == 1) {
        record.putValue(TIMESTAMP_COLUMN, new Timestamp(timestamp));
      } else {
        record.putValue(TIMESTAMP_COLUMN, new Timestamp(timestamp).toString());
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
      BrokerResponseNative brokerResponse = getBrokerResponseForSqlQuery(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{"timestampColumn"}, new ColumnDataType[]{ColumnDataType.TIMESTAMP}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      for (int i = 0; i < 10; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 1);
        assertEquals(row[0], new Timestamp(BASE_TIMESTAMP + i).toString());
      }
    }
    {
      String query = "SELECT * FROM testTable ORDER BY timestampColumn DESC LIMIT 40";
      BrokerResponseNative brokerResponse = getBrokerResponseForSqlQuery(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{"timestampColumn"}, new ColumnDataType[]{ColumnDataType.TIMESTAMP}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 40);
      for (int i = 0; i < 10; i++) {
        String expectedResult = new Timestamp(BASE_TIMESTAMP + NUM_RECORDS - 1 - i).toString();
        for (int j = 0; j < 4; j++) {
          Object[] row = rows.get(i * 4 + j);
          assertEquals(row.length, 1);
          assertEquals(row[0], expectedResult);
        }
      }
    }
    {
      String query =
          "SELECT FROM_TIMESTAMP(timestampColumn) AS longTimestamp FROM testTable WHERE timestampColumn > '2021-01-01 00:00:00.123' AND timestampColumn >= "
              + (BASE_TIMESTAMP + 234);
      BrokerResponseNative brokerResponse = getBrokerResponseForSqlQuery(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{"longTimestamp"}, new ColumnDataType[]{ColumnDataType.LONG}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      for (int i = 0; i < 10; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 1);
        assertEquals(row[0], BASE_TIMESTAMP + 234 + i);
      }
    }
    {
      String query = "SELECT DISTINCT timestampColumn FROM testTable ORDER BY timestampColumn";
      BrokerResponseNative brokerResponse = getBrokerResponseForSqlQuery(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{"timestampColumn"}, new ColumnDataType[]{ColumnDataType.TIMESTAMP}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      for (int i = 0; i < 10; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 1);
        assertEquals(row[0], new Timestamp(BASE_TIMESTAMP + i).toString());
      }
    }
    {
      String query =
          "SELECT COUNT(*) AS count, timestampColumn FROM testTable GROUP BY timestampColumn ORDER BY timestampColumn DESC";
      BrokerResponseNative brokerResponse = getBrokerResponseForSqlQuery(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema, new DataSchema(new String[]{"count", "timestampColumn"},
          new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.TIMESTAMP}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 10);
      for (int i = 0; i < 10; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 2);
        assertEquals(row[0], 4L);
        assertEquals(row[1], new Timestamp(BASE_TIMESTAMP + NUM_RECORDS - i - 1).toString());
      }
    }
    {
      String query =
          "SELECT TO_TIMESTAMP(MAX(timestampColumn)) AS maxTimestamp FROM testTable GROUP BY timestampColumn HAVING maxTimestamp < '2021-01-01 00:00:00.005' ORDER BY maxTimestamp";
      BrokerResponseNative brokerResponse = getBrokerResponseForSqlQuery(query);
      ResultTable resultTable = brokerResponse.getResultTable();
      DataSchema dataSchema = resultTable.getDataSchema();
      assertEquals(dataSchema,
          new DataSchema(new String[]{"maxTimestamp"}, new ColumnDataType[]{ColumnDataType.TIMESTAMP}));
      List<Object[]> rows = resultTable.getRows();
      assertEquals(rows.size(), 5);
      for (int i = 0; i < 5; i++) {
        Object[] row = rows.get(i);
        assertEquals(row.length, 1);
        assertEquals(row[0], new Timestamp(BASE_TIMESTAMP + i).toString());
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
