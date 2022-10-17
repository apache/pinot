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
package org.apache.pinot.plugin.inputformat.avro;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.AbstractRecordExtractorTest;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.avro.Schema.*;


/**
 * Tests the {@link AvroRecordExtractor} using a schema containing groovy transform functions
 */
public class AvroRecordExtractorTest extends AbstractRecordExtractorTest {
  private final File _dataFile = new File(_tempDir, "events.avro");

  /**
   * Create an AvroRecordReader
   */
  @Override
  protected RecordReader createRecordReader(Set<String> fieldsToRead)
      throws IOException {
    AvroRecordReader avroRecordReader = new AvroRecordReader();
    avroRecordReader.init(_dataFile, fieldsToRead, new AvroRecordReaderConfig());
    return avroRecordReader;
  }

  /**
   * Create an Avro input file using the input records
   */
  @Override
  protected void createInputFile()
      throws IOException {

    Schema avroSchema = createRecord("eventsRecord", null, null, false);
    List<Field> fields = Arrays.asList(
        new Field("user_id", createUnion(Lists.newArrayList(create(Type.INT), create(Type.NULL))), null, null),
        new Field("firstName", createUnion(Lists.newArrayList(create(Type.STRING), create(Type.NULL))), null, null),
        new Field("lastName", createUnion(Lists.newArrayList(create(Type.STRING), create(Type.NULL))), null, null),
        new Field("bids", createUnion(Lists.newArrayList(createArray(create(Type.INT)), create(Type.NULL))), null,
            null), new Field("campaignInfo", create(Type.STRING), null, null),
        new Field("cost", create(Type.DOUBLE), null, null), new Field("timestamp", create(Type.LONG), null, null),
        new Field("xarray", createArray(create(Type.STRING))), new Field("xmap", createMap(create(Type.STRING))));

    avroSchema.setFields(fields);

    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, _dataFile);
      for (Map<String, Object> inputRecord : _inputRecords) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        for (String columnName : _sourceFieldNames) {
          record.put(columnName, inputRecord.get(columnName));
        }
        fileWriter.append(record);
      }
    }
  }

  @Test
  public void testDataTypeReturnFromAvroRecordExtractor()
      throws IOException {
    String testColumnName = "column1";
    long columnValue = 999999999L;
    AvroRecordExtractor avroRecordExtractor = new AvroRecordExtractor();
    avroRecordExtractor.init(null, new AvroRecordExtractorConfig());

    org.apache.pinot.spi.data.Schema pinotSchema =
        new org.apache.pinot.spi.data.Schema.SchemaBuilder().addSingleValueDimension(testColumnName,
            FieldSpec.DataType.LONG).build();
    Schema schema = AvroUtils.getAvroSchemaFromPinotSchema(pinotSchema);
    GenericRecord genericRecord = new GenericData.Record(schema);
    genericRecord.put(testColumnName, columnValue);
    GenericRow genericRow = new GenericRow();

    avroRecordExtractor.extract(genericRecord, genericRow);
    Assert.assertEquals(columnValue, genericRow.getValue(testColumnName));
    Assert.assertEquals("Long", genericRow.getValue(testColumnName).getClass().getSimpleName());

    String jsonString = genericRecord.toString();
    Map<String, Object> jsonMap = JsonUtils.stringToObject(jsonString, JsonUtils.MAP_TYPE_REFERENCE);
    // The data type got changed to Integer, which will then have to trigger the convert method in
    // DataTypeTransformer class.
    Assert.assertEquals("Integer", jsonMap.get(testColumnName).getClass().getSimpleName());
  }

  @Test
  public void testDataTypeReturnFromAvroRecordExtractorUsingLogicalType() {
    String bytesColName = "column1";
    byte[] bytesColValue = "ABC".getBytes(StandardCharsets.UTF_8);
    String bigDecimalColName = "column2";
    double bigDecimalDoubleValue = 1.999999999D;
    BigDecimal bigDecimalLogicalValue = new BigDecimal(bigDecimalDoubleValue, MathContext.DECIMAL64).setScale(10);
    BigDecimal bigDecimalColValue = bigDecimalLogicalValue;
    String uuidColName = "column3";
    String uuidColValue = "af68efc2-818a-42ac-96c3-ced5ca6585a2";
    UUID uuidColLogicalValue = UUID.fromString(uuidColValue);
    String dateColName = "column4";
    String dateColValue = "2022-04-14";
    LocalDate dateColLogicalValue = LocalDate.of(2022, 4, 14);
    String timeMillisColName = "column5";
    String timeMillisColValue = "08:51:32.123";
    LocalTime timeMillisColLogicalValue = LocalTime.parse(timeMillisColValue);
    String timeMicrosColName = "column6";
    String timeMicrosColValue = "08:51:32.123987";
    LocalTime timeMicrosColLogicalValue = LocalTime.parse(timeMicrosColValue);
    String timestampMillisColName = "column7";
    Instant timestampMillisColLogicalValue = Instant.ofEpochMilli(1649924302123L);
    Timestamp timestampColValue = new Timestamp(1649924302000L);
    timestampColValue.setNanos(123000000);
    String timestampMicrosColName = "column8";
    Instant timestampMicrosColLogicalValue = Instant.ofEpochMilli(1649924302123L).plus(987, ChronoUnit.MICROS);
    Timestamp timestampMicrosColValue = new Timestamp(1649924302000L);
    timestampMicrosColValue.setNanos(123987000);
    AvroRecordExtractorConfig config = new AvroRecordExtractorConfig();
    config.setEnableLogicalTypes(true);
    AvroRecordExtractor avroRecordExtractor = new AvroRecordExtractor();
    avroRecordExtractor.init(null, config);

    String schemaString =
        new StringBuilder().append("{").append("  \"type\": \"record\",").append("  \"name\": \"test\",")
            .append("  \"fields\": [{").append("    \"name\": \"column1\",").append("    \"type\": \"bytes\"")
            .append("  },{").append("    \"name\": \"column2\",").append("    \"type\": {")
            .append("      \"type\": \"bytes\",").append("      \"logicalType\": \"decimal\",")
            .append("      \"precision\": 64,").append("      \"scale\": 10").append("    }").append("  },{")
            .append("    \"name\": \"column3\",").append("    \"type\": {").append("      \"type\": \"string\",")
            .append("      \"logicalType\": \"uuid\"").append("    }").append("  },{")
            .append("    \"name\": \"column4\",").append("    \"type\": {").append("      \"type\": \"int\",")
            .append("      \"logicalType\": \"date\"").append("    }").append("  },{")
            .append("    \"name\": \"column5\",").append("    \"type\": {").append("      \"type\": \"int\",")
            .append("      \"logicalType\": \"time-millis\"").append("    }").append("  },{")
            .append("    \"name\": \"column6\",").append("    \"type\": {").append("      \"type\": \"long\",")
            .append("      \"logicalType\": \"time-micros\"").append("    }").append("  },{")
            .append("    \"name\": \"column7\",").append("    \"type\": {").append("      \"type\": \"long\",")
            .append("      \"logicalType\": \"timestamp-millis\"").append("    }").append("  },{")
            .append("    \"name\": \"column8\",").append("    \"type\": {").append("      \"type\": \"long\",")
            .append("      \"logicalType\": \"timestamp-micros\"").append("    }").append("  }]").append("}")
            .toString();
    Schema schema = new Schema.Parser().parse(schemaString);
    GenericRecord genericRecord = new GenericData.Record(schema);
    genericRecord.put(bytesColName, bytesColValue);
    Schema fieldSchema = schema.getField(bigDecimalColName).schema();
    Object datum = Conversions.convertToRawType(bigDecimalLogicalValue, fieldSchema, fieldSchema.getLogicalType(),
        new Conversions.DecimalConversion());
    genericRecord.put(bigDecimalColName, datum);
    fieldSchema = schema.getField(uuidColName).schema();
    datum = Conversions.convertToRawType(uuidColLogicalValue, fieldSchema, fieldSchema.getLogicalType(),
        new Conversions.UUIDConversion());
    genericRecord.put(uuidColName, datum);
    fieldSchema = schema.getField(dateColName).schema();
    datum = Conversions.convertToRawType(dateColLogicalValue, fieldSchema, fieldSchema.getLogicalType(),
        new TimeConversions.DateConversion());
    genericRecord.put(dateColName, datum);
    fieldSchema = schema.getField(timeMillisColName).schema();
    datum = Conversions.convertToRawType(timeMillisColLogicalValue, fieldSchema, fieldSchema.getLogicalType(),
        new TimeConversions.TimeMillisConversion());
    genericRecord.put(timeMillisColName, datum);
    fieldSchema = schema.getField(timeMicrosColName).schema();
    datum = Conversions.convertToRawType(timeMicrosColLogicalValue, fieldSchema, fieldSchema.getLogicalType(),
        new TimeConversions.TimeMicrosConversion());
    genericRecord.put(timeMicrosColName, datum);
    fieldSchema = schema.getField(timestampMillisColName).schema();
    datum = Conversions.convertToRawType(timestampMillisColLogicalValue, fieldSchema, fieldSchema.getLogicalType(),
        new TimeConversions.TimestampMillisConversion());
    genericRecord.put(timestampMillisColName, datum);
    fieldSchema = schema.getField(timestampMicrosColName).schema();
    datum = Conversions.convertToRawType(timestampMicrosColLogicalValue, fieldSchema, fieldSchema.getLogicalType(),
        new TimeConversions.TimestampMicrosConversion());
    genericRecord.put(timestampMicrosColName, datum);
    GenericRow genericRow = new GenericRow();

    avroRecordExtractor.extract(genericRecord, genericRow);
    Assert.assertEquals(genericRow.getValue(bytesColName), bytesColValue);
    Assert.assertEquals(genericRow.getValue(bytesColName).getClass().getSimpleName(), "byte[]");
    Assert.assertEquals(genericRow.getValue(bigDecimalColName), bigDecimalColValue);
    Assert.assertEquals(genericRow.getValue(bigDecimalColName).getClass().getSimpleName(), "BigDecimal");
    Assert.assertEquals(genericRow.getValue(uuidColName), uuidColValue);
    Assert.assertEquals(genericRow.getValue(uuidColName).getClass().getSimpleName(), "String");
    Assert.assertEquals(genericRow.getValue(dateColName), dateColValue);
    Assert.assertEquals(genericRow.getValue(dateColName).getClass().getSimpleName(), "String");
    Assert.assertEquals(genericRow.getValue(timeMillisColName), timeMillisColValue);
    Assert.assertEquals(genericRow.getValue(timeMillisColName).getClass().getSimpleName(), "String");
    Assert.assertEquals(genericRow.getValue(timeMicrosColName), timeMicrosColValue);
    Assert.assertEquals(genericRow.getValue(timeMicrosColName).getClass().getSimpleName(), "String");
    Assert.assertEquals(genericRow.getValue(timestampMillisColName), timestampColValue);
    Assert.assertEquals(genericRow.getValue(timestampMillisColName).getClass().getSimpleName(), "Timestamp");
    Assert.assertEquals(genericRow.getValue(timestampMicrosColName), timestampMicrosColValue);
    Assert.assertEquals(genericRow.getValue(timestampMicrosColName).getClass().getSimpleName(), "Timestamp");
  }

  @Test
  public void testReusedByteBuffer() {
    byte[] content = new byte[100];
    ThreadLocalRandom.current().nextBytes(content);
    ByteBuffer byteBuffer = ByteBuffer.wrap(content);
    AvroRecordExtractor avroRecordExtractor = new AvroRecordExtractor();
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(avroRecordExtractor.convertSingleValue(byteBuffer), content);
    }
  }
}
