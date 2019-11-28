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
package org.apache.pinot.core.data.readers;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.core.data.recordtransformer.CompositeTransformer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class RecordReaderSampleDataTest {
  private static final File AVRO_SAMPLE_DATA_FILE = new File(Preconditions
      .checkNotNull(RecordReaderSampleDataTest.class.getClassLoader().getResource("data/test_sample_data.avro"))
      .getFile());
  private static final File CSV_SAMPLE_DATA_FILE = new File(Preconditions
      .checkNotNull(RecordReaderSampleDataTest.class.getClassLoader().getResource("data/test_sample_data.csv"))
      .getFile());
  private static final File JSON_SAMPLE_DATA_FILE = new File(Preconditions
      .checkNotNull(RecordReaderSampleDataTest.class.getClassLoader().getResource("data/test_sample_data.json"))
      .getFile());
  private final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension("column1", FieldSpec.DataType.LONG)
      .addSingleValueDimension("column2", FieldSpec.DataType.INT)
      .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
      .addSingleValueDimension("column7", FieldSpec.DataType.STRING)
      .addSingleValueDimension("unknown_dimension", FieldSpec.DataType.STRING)
      .addMetric("met_impressionCount", FieldSpec.DataType.LONG).addMetric("unknown_metric", FieldSpec.DataType.DOUBLE)
      .build();
  // Same incoming and outgoing time column name, should read value with the incoming time data type
  private final Schema SCHEMA_SAME_INCOMING_OUTGOING = new Schema.SchemaBuilder()
      .addTime("time_day", TimeUnit.SECONDS, FieldSpec.DataType.LONG, "time_day", TimeUnit.DAYS, FieldSpec.DataType.INT)
      .build();
  // Different incoming and outgoing time column name, should read both incoming and outgoing time
  private final Schema SCHEMA_DIFFERENT_INCOMING_OUTGOING = new Schema.SchemaBuilder()
      .addTime("time_day", TimeUnit.SECONDS, FieldSpec.DataType.LONG, "column2", TimeUnit.DAYS, FieldSpec.DataType.INT)
      .build();
  // Incoming time column does not exist in the record, should read outgoing time only
  private final Schema SCHEMA_NO_INCOMING = new Schema.SchemaBuilder()
      .addTime("incoming", TimeUnit.SECONDS, FieldSpec.DataType.LONG, "time_day", TimeUnit.DAYS, FieldSpec.DataType.INT)
      .build();
  // Outgoing time column does not exist in the record, should read incoming time only
  private final Schema SCHEMA_NO_OUTGOING = new Schema.SchemaBuilder()
      .addTime("time_day", TimeUnit.SECONDS, FieldSpec.DataType.LONG, "outgoing", TimeUnit.DAYS, FieldSpec.DataType.INT)
      .build();

  @Test
  public void testRecordReaders()
      throws Exception {
    CompositeTransformer defaultTransformer = CompositeTransformer.getDefaultTransformer(SCHEMA);
    try (AvroRecordReader avroRecordReader = new AvroRecordReader(AVRO_SAMPLE_DATA_FILE, SCHEMA);
        CSVRecordReader csvRecordReader = new CSVRecordReader(CSV_SAMPLE_DATA_FILE, SCHEMA, null);
        JSONRecordReader jsonRecordReader = new JSONRecordReader(JSON_SAMPLE_DATA_FILE, SCHEMA)) {
      int numRecords = 0;
      while (avroRecordReader.hasNext()) {
        assertTrue(csvRecordReader.hasNext());
        assertTrue(jsonRecordReader.hasNext());
        numRecords++;

        GenericRow avroRecord = defaultTransformer.transform(avroRecordReader.next());
        GenericRow csvRecord = defaultTransformer.transform(csvRecordReader.next());
        GenericRow jsonRecord = defaultTransformer.transform(jsonRecordReader.next());
        assertEquals(avroRecord, csvRecord);
        assertEquals(avroRecord, jsonRecord);

        // Check the values from the first record
        if (numRecords == 1) {
          // Dimensions
          assertEquals(avroRecord.getValue("column1"), 1840748525967736008L);
          assertEquals(avroRecord.getValue("column2"), 231355578);
          assertEquals(avroRecord.getValue("column3"), "CezOib");

          // Dimension empty string
          assertEquals(avroRecord.getValue("column7"), "");

          // Dimension default column
          assertEquals(avroRecord.getValue("unknown_dimension"), "null");

          // Metric
          assertEquals(avroRecord.getValue("met_impressionCount"), 4955241829510629137L);

          // Metric default column
          assertEquals(avroRecord.getValue("unknown_metric"), 0.0);
        }
      }
      assertEquals(numRecords, 10001);
    }
  }

  @Test
  public void testSameIncomingOutgoing()
      throws Exception {
    try (AvroRecordReader avroRecordReader = new AvroRecordReader(AVRO_SAMPLE_DATA_FILE, SCHEMA_SAME_INCOMING_OUTGOING);
        CSVRecordReader csvRecordReader = new CSVRecordReader(CSV_SAMPLE_DATA_FILE, SCHEMA_SAME_INCOMING_OUTGOING,
            null); JSONRecordReader jsonRecordReader = new JSONRecordReader(JSON_SAMPLE_DATA_FILE,
        SCHEMA_SAME_INCOMING_OUTGOING)) {
      int numRecords = 0;
      while (avroRecordReader.hasNext()) {
        assertTrue(csvRecordReader.hasNext());
        assertTrue(jsonRecordReader.hasNext());
        numRecords++;

        GenericRow avroRecord = avroRecordReader.next();
        GenericRow csvRecord = csvRecordReader.next();
        GenericRow jsonRecord = jsonRecordReader.next();
        assertEquals(avroRecord, csvRecord);
        assertEquals(avroRecord, jsonRecord);

        // Check the values from the first record
        if (numRecords == 1) {
          // Should be in incoming time data type (LONG)
          assertEquals(avroRecord.getValue("time_day"), 1072889503L);
        }
      }
      assertEquals(numRecords, 10001);
    }
  }

  @Test
  public void testDifferentIncomingOutgoing()
      throws Exception {
    try (AvroRecordReader avroRecordReader = new AvroRecordReader(AVRO_SAMPLE_DATA_FILE,
        SCHEMA_DIFFERENT_INCOMING_OUTGOING);
        CSVRecordReader csvRecordReader = new CSVRecordReader(CSV_SAMPLE_DATA_FILE, SCHEMA_DIFFERENT_INCOMING_OUTGOING,
            null); JSONRecordReader jsonRecordReader = new JSONRecordReader(JSON_SAMPLE_DATA_FILE,
        SCHEMA_DIFFERENT_INCOMING_OUTGOING)) {
      int numRecords = 0;
      while (avroRecordReader.hasNext()) {
        assertTrue(csvRecordReader.hasNext());
        assertTrue(jsonRecordReader.hasNext());
        numRecords++;

        GenericRow avroRecord = avroRecordReader.next();
        GenericRow csvRecord = csvRecordReader.next();
        GenericRow jsonRecord = jsonRecordReader.next();
        assertEquals(avroRecord, csvRecord);
        assertEquals(avroRecord, jsonRecord);

        // Check the values from the first record
        if (numRecords == 1) {
          // Incoming time column
          assertEquals(avroRecord.getValue("time_day"), 1072889503L);

          // Outgoing time column
          assertEquals(avroRecord.getValue("column2"), 231355578);
        }
      }
      assertEquals(numRecords, 10001);
    }
  }

  @Test
  public void testNoIncoming()
      throws Exception {
    try (AvroRecordReader avroRecordReader = new AvroRecordReader(AVRO_SAMPLE_DATA_FILE, SCHEMA_NO_INCOMING);
        CSVRecordReader csvRecordReader = new CSVRecordReader(CSV_SAMPLE_DATA_FILE, SCHEMA_NO_INCOMING, null);
        JSONRecordReader jsonRecordReader = new JSONRecordReader(JSON_SAMPLE_DATA_FILE, SCHEMA_NO_INCOMING)) {
      int numRecords = 0;
      while (avroRecordReader.hasNext()) {
        assertTrue(csvRecordReader.hasNext());
        assertTrue(jsonRecordReader.hasNext());
        numRecords++;

        GenericRow avroRecord = avroRecordReader.next();
        GenericRow csvRecord = csvRecordReader.next();
        GenericRow jsonRecord = jsonRecordReader.next();
        assertEquals(avroRecord, csvRecord);
        assertEquals(avroRecord, jsonRecord);

        // Check the values from the first record
        if (numRecords == 1) {
          // Incoming time column should be null
          assertNull(avroRecord.getValue("incoming"));

          // Outgoing time column
          assertEquals(avroRecord.getValue("time_day"), 1072889503);
        }
      }
      assertEquals(numRecords, 10001);
    }
  }

  @Test
  public void testNoOutgoing()
      throws Exception {
    try (AvroRecordReader avroRecordReader = new AvroRecordReader(AVRO_SAMPLE_DATA_FILE, SCHEMA_NO_OUTGOING);
        CSVRecordReader csvRecordReader = new CSVRecordReader(CSV_SAMPLE_DATA_FILE, SCHEMA_NO_OUTGOING, null);
        JSONRecordReader jsonRecordReader = new JSONRecordReader(JSON_SAMPLE_DATA_FILE, SCHEMA_NO_OUTGOING)) {
      int numRecords = 0;
      while (avroRecordReader.hasNext()) {
        assertTrue(csvRecordReader.hasNext());
        assertTrue(jsonRecordReader.hasNext());
        numRecords++;

        GenericRow avroRecord = avroRecordReader.next();
        GenericRow csvRecord = csvRecordReader.next();
        GenericRow jsonRecord = jsonRecordReader.next();
        assertEquals(avroRecord, csvRecord);
        assertEquals(avroRecord, jsonRecord);

        // Check the values from the first record
        if (numRecords == 1) {
          // Incoming time column
          assertEquals(avroRecord.getValue("time_day"), 1072889503L);

          // Outgoing time column should be null
          assertNull(avroRecord.getValue("outgoing"));
        }
      }
      assertEquals(numRecords, 10001);
    }
  }
}
