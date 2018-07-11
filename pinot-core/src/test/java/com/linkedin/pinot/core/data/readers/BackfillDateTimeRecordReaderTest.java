/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.readers;

import com.linkedin.pinot.common.data.DateTimeFieldSpec;
import com.linkedin.pinot.common.data.DateTimeFormatSpec;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.minion.BackfillDateTimeColumn;
import com.linkedin.pinot.core.minion.BackfillDateTimeColumn.BackfillDateTimeRecordReader;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests the PinotSegmentRecordReader to check that the records being generated
 * are the same as the records used to create the segment
 */
public class BackfillDateTimeRecordReaderTest {
  private static final int NUM_ROWS = 10000;

  private static String D1 = "d1";
  private static String D2 = "d2";
  private static String M1 = "m1";
  private static String M2 = "m2";

  private List<GenericRow> createTestDataWithTimespec(TimeFieldSpec timeFieldSpec) {
    List<GenericRow> rows = new ArrayList<>();
    Random random = new Random();

    Map<String, Object> fields;
    for (int i = 0; i < NUM_ROWS; i++) {
      fields = new HashMap<>();
      fields.put(D1, RandomStringUtils.randomAlphabetic(2));
      fields.put(D2, RandomStringUtils.randomAlphabetic(5));
      fields.put(M1, Math.abs(random.nextInt()));
      fields.put(M2, Math.abs(random.nextFloat()));

      long timestamp = System.currentTimeMillis();
      Object timeColumnValue = timeFieldSpec.getIncomingGranularitySpec().fromMillis(timestamp);
      fields.put(timeFieldSpec.getName(), timeColumnValue);

      GenericRow row = new GenericRow();
      row.init(fields);
      rows.add(row);
    }
    return rows;
  }

  private List<GenericRow> createTestDataWithTimespec(TimeFieldSpec timeFieldSpec,
      DateTimeFieldSpec dateTimeFieldSpec) {
    List<GenericRow> rows = new ArrayList<>();
    Random random = new Random();

    Map<String, Object> fields;
    for (int i = 0; i < NUM_ROWS; i++) {
      fields = new HashMap<>();
      fields.put(D1, RandomStringUtils.randomAlphabetic(2));
      fields.put(D2, RandomStringUtils.randomAlphabetic(5));
      fields.put(M1, Math.abs(random.nextInt()));
      fields.put(M2, Math.abs(random.nextFloat()));

      long timestamp = System.currentTimeMillis();
      Object timeColumnValue = timeFieldSpec.getIncomingGranularitySpec().fromMillis(timestamp);
      fields.put(timeFieldSpec.getName(), timeColumnValue);

      DateTimeFormatSpec toFormat = new DateTimeFormatSpec(dateTimeFieldSpec.getFormat());
      Object dateTimeColumnValue = toFormat.fromMillisToFormat(timestamp, Object.class);
      fields.put(dateTimeFieldSpec.getName(), dateTimeColumnValue);

      GenericRow row = new GenericRow();
      row.init(fields);
      rows.add(row);
    }
    return rows;
  }

  private Schema createPinotSchemaWithTimeSpec(TimeFieldSpec timeSpec) {
    Schema testSchema = new Schema();
    testSchema.setSchemaName("schema");
    FieldSpec spec;
    spec = new DimensionFieldSpec(D1, DataType.STRING, true);
    testSchema.addField(spec);
    spec = new DimensionFieldSpec(D2, DataType.STRING, true);
    testSchema.addField(spec);
    spec = new MetricFieldSpec(M1, DataType.INT);
    testSchema.addField(spec);
    spec = new MetricFieldSpec(M2, DataType.FLOAT);
    testSchema.addField(spec);
    testSchema.addField(timeSpec);
    return testSchema;
  }

  private Schema createPinotSchemaWithTimeSpec(TimeFieldSpec timeSpec, DateTimeFieldSpec dateTimeFieldSpec) {
    Schema testSchema = createPinotSchemaWithTimeSpec(timeSpec);
    testSchema.addField(dateTimeFieldSpec);
    return testSchema;
  }

  private Schema createPinotSchemaWrapperWithDateTimeSpec(Schema schema, DateTimeFieldSpec dateTimeFieldSpec) {
    schema.addField(dateTimeFieldSpec);
    return schema;
  }

  @Test(dataProvider = "backfillRecordReaderDataProvider")
  public void testBackfillDateTimeRecordReader(RecordReader baseRecordReader, TimeFieldSpec timeFieldSpec,
      DateTimeFieldSpec dateTimeFieldSpec, Schema schemaExpected) throws Exception {
    BackfillDateTimeColumn backfillDateTimeColumn =
        new BackfillDateTimeColumn(new File("original"), new File("backup"), timeFieldSpec, dateTimeFieldSpec);
    try (BackfillDateTimeRecordReader wrapperReader = backfillDateTimeColumn.getBackfillDateTimeRecordReader(
        baseRecordReader)) {

      // check that schema has new column
      Schema schemaActual = wrapperReader.getSchema();
      Assert.assertEquals(schemaActual, schemaExpected);

      DateTimeFieldSpec dateTimeFieldSpecActual = schemaActual.getDateTimeSpec(dateTimeFieldSpec.getName());
      TimeFieldSpec timeFieldSpecActual = schemaActual.getTimeFieldSpec();
      Assert.assertEquals(dateTimeFieldSpecActual, dateTimeFieldSpec);
      Assert.assertEquals(timeFieldSpecActual, timeFieldSpec);

      while (wrapperReader.hasNext()) {
        GenericRow next = wrapperReader.next();

        // check that new datetime column is generated
        Object dateTimeColumnValueActual = next.getValue(dateTimeFieldSpec.getName());
        Assert.assertNotNull(dateTimeColumnValueActual);

        Object timeColumnValueActual = next.getValue(timeFieldSpec.getName());
        Assert.assertNotNull(timeColumnValueActual);

        // check that datetime column has correct value as per its format
        Long timeColumnValueMS = timeFieldSpec.getIncomingGranularitySpec().toMillis(timeColumnValueActual);
        DateTimeFormatSpec toFormat = new DateTimeFormatSpec(dateTimeFieldSpec.getFormat());
        Object dateTimeColumnValueExpected = toFormat.fromMillisToFormat(timeColumnValueMS, Object.class);
        Assert.assertEquals(dateTimeColumnValueActual, dateTimeColumnValueExpected);
      }
    }
  }

  @DataProvider(name = "backfillRecordReaderDataProvider")
  public Object[][] getDataForTestBackfillRecordReader() throws Exception {
    List<Object[]> entries = new ArrayList<>();

    List<GenericRow> inputData;
    Schema inputSchema;
    RecordReader inputRecordReader;
    TimeFieldSpec timeFieldSpec;
    DateTimeFieldSpec dateTimeFieldSpec;
    Schema wrapperSchema;

    // timeSpec in hoursSinceEpoch, generate dateTimeFieldSpec in millisSinceEpoch
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "Date"));
    inputData = createTestDataWithTimespec(timeFieldSpec);
    inputSchema = createPinotSchemaWithTimeSpec(timeFieldSpec);
    inputRecordReader = new GenericRowRecordReader(inputData, inputSchema);
    dateTimeFieldSpec = new DateTimeFieldSpec("timestampInEpoch", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS");
    wrapperSchema = createPinotSchemaWrapperWithDateTimeSpec(inputSchema, dateTimeFieldSpec);
    entries.add(new Object[]{inputRecordReader, timeFieldSpec, dateTimeFieldSpec, wrapperSchema});

    // timeSpec in hoursSinceEpoch, generate dateTimeFieldSpec in sdf day
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "Date"));
    inputData = createTestDataWithTimespec(timeFieldSpec);
    inputSchema = createPinotSchemaWithTimeSpec(timeFieldSpec);
    inputRecordReader = new GenericRowRecordReader(inputData, inputSchema);
    dateTimeFieldSpec =
        new DateTimeFieldSpec("timestampInEpoch", DataType.LONG, "1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd", "1:HOURS");
    wrapperSchema = createPinotSchemaWrapperWithDateTimeSpec(inputSchema, dateTimeFieldSpec);
    entries.add(new Object[]{inputRecordReader, timeFieldSpec, dateTimeFieldSpec, wrapperSchema});

    // timeSpec in hoursSinceEpoch, generate dateTimeFieldSpec in hoursSinceEpoch
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "Date"));
    inputData = createTestDataWithTimespec(timeFieldSpec);
    inputSchema = createPinotSchemaWithTimeSpec(timeFieldSpec);
    inputRecordReader = new GenericRowRecordReader(inputData, inputSchema);
    dateTimeFieldSpec =
        new DateTimeFieldSpec("timestampInEpoch", DataType.LONG, "1:HOURS:EPOCH", "1:HOURS");
    wrapperSchema = createPinotSchemaWrapperWithDateTimeSpec(inputSchema, dateTimeFieldSpec);
    entries.add(new Object[]{inputRecordReader, timeFieldSpec, dateTimeFieldSpec, wrapperSchema});

    // timeSpec in millisSinceEpoch, generate dateTimeFieldSpec in 5 minutesSinceEpoch
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "Date"));
    inputData = createTestDataWithTimespec(timeFieldSpec);
    inputSchema = createPinotSchemaWithTimeSpec(timeFieldSpec);
    inputRecordReader = new GenericRowRecordReader(inputData, inputSchema);
    dateTimeFieldSpec = new DateTimeFieldSpec("timestampInEpoch", DataType.LONG, "5:MILLISECONDS:EPOCH", "1:HOURS");
    wrapperSchema = createPinotSchemaWrapperWithDateTimeSpec(inputSchema, dateTimeFieldSpec);
    entries.add(new Object[]{inputRecordReader, timeFieldSpec, dateTimeFieldSpec, wrapperSchema});

    // timeSpec in hoursSinceEpoch, dateTimeFieldSpec in millisSinceEpoch, override dateTimeFieldSpec in millisSinceEpoch
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "Date"));
    dateTimeFieldSpec = new DateTimeFieldSpec("timestampInEpoch", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS");
    inputData = createTestDataWithTimespec(timeFieldSpec, dateTimeFieldSpec);
    inputSchema = createPinotSchemaWithTimeSpec(timeFieldSpec, dateTimeFieldSpec);
    inputRecordReader = new GenericRowRecordReader(inputData, inputSchema);
    entries.add(new Object[]{inputRecordReader, timeFieldSpec, dateTimeFieldSpec, inputSchema});

    // timeSpec in hoursSinceEpoch, dateTimeFieldSpec in hoursSinceEpoch, override dateTimeFieldSpec in millisSinceEpoch
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "Date"));
    dateTimeFieldSpec = new DateTimeFieldSpec("timestampInEpoch", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS");
    inputData = createTestDataWithTimespec(timeFieldSpec, dateTimeFieldSpec);
    inputSchema = createPinotSchemaWithTimeSpec(timeFieldSpec, dateTimeFieldSpec);
    inputRecordReader = new GenericRowRecordReader(inputData, inputSchema);
    dateTimeFieldSpec = new DateTimeFieldSpec("timestampInEpoch", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS");
    wrapperSchema = createPinotSchemaWithTimeSpec(timeFieldSpec, dateTimeFieldSpec);
    entries.add(new Object[]{inputRecordReader, timeFieldSpec, dateTimeFieldSpec, wrapperSchema});

    // timeSpec in hoursSinceEpoch, dateTimeFieldSpec in hoursSinceEpoch, add new dateTimeFieldSpec in millisSinceEpoch
    timeFieldSpec = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "Date"));
    dateTimeFieldSpec =
        new DateTimeFieldSpec("hoursSinceEpoch", DataType.LONG, "1:HOURS:EPOCH", "1:HOURS");
    inputData = createTestDataWithTimespec(timeFieldSpec, dateTimeFieldSpec);
    inputSchema = createPinotSchemaWithTimeSpec(timeFieldSpec, dateTimeFieldSpec);
    inputRecordReader = new GenericRowRecordReader(inputData, inputSchema);
    DateTimeFieldSpec dateTimeFieldSpecNew =
        new DateTimeFieldSpec("timestampInEpoch", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:HOURS");
    wrapperSchema = createPinotSchemaWithTimeSpec(timeFieldSpec, dateTimeFieldSpec);
    wrapperSchema = createPinotSchemaWrapperWithDateTimeSpec(wrapperSchema, dateTimeFieldSpecNew);
    entries.add(new Object[]{inputRecordReader, timeFieldSpec, dateTimeFieldSpecNew, wrapperSchema});

    return entries.toArray(new Object[entries.size()][]);
  }
}
