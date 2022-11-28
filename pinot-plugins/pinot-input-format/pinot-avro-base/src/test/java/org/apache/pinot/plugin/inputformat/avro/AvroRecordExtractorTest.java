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
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.specific.SpecificData;
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

  @Test
  public void testGenericFixedDataType() {
    Schema avroSchema = createRecord("EventRecord", null, null, false);
    Schema fixedSchema = createFixed("FixedSchema", "", "", 4);
    avroSchema.setFields(Lists.newArrayList(new Schema.Field("fixedData", fixedSchema)));
    GenericRecord genericRecord = new GenericData.Record(avroSchema);
    genericRecord.put("fixedData", new GenericData.Fixed(fixedSchema, new byte[]{0, 1, 2, 3}));
    GenericRow genericRow = new GenericRow();
    AvroRecordExtractor avroRecordExtractor = new AvroRecordExtractor();
    avroRecordExtractor.init(null, null);
    avroRecordExtractor.extract(genericRecord, genericRow);
    Assert.assertEquals(genericRow.getValue("fixedData"), new byte[]{0, 1, 2, 3});
  }

  @Test
  public void testSpecificFixedDataType() {
    EventRecord specificRecord = new EventRecord(new FixedSchema(new byte[]{0, 1, 2, 3}));
    GenericRow outputGenericRow = new GenericRow();
    AvroRecordExtractor avroRecordExtractor = new AvroRecordExtractor();
    avroRecordExtractor.init(null, null);
    avroRecordExtractor.extract(specificRecord, outputGenericRow);
    Assert.assertEquals(outputGenericRow.getValue("fixedData"), new byte[]{0, 1, 2, 3});
  }

  /**
   * SpecificRecord created for testing Fixed data type
   */
  static class EventRecord extends org.apache.avro.specific.SpecificRecordBase
      implements org.apache.avro.specific.SpecificRecord {
    private static final long serialVersionUID = 5451592186784305712L;
    public static final org.apache.avro.Schema SCHEMA = new org.apache.avro.Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"EventRecord\",\"fields\":[{"
            + "\"name\":\"fixedData\",\"type\":{\"type\":\"fixed\",\"name\":\"FixedSchema\",\"doc\":\"\",\"size\":4}}"
            + "]}");

    public static org.apache.avro.Schema getClassSchema() {
      return SCHEMA;
    }

    private static final SpecificData MODEL = new SpecificData();

    private static final BinaryMessageEncoder<EventRecord> ENCODER =
        new BinaryMessageEncoder<EventRecord>(MODEL, SCHEMA);

    private static final BinaryMessageDecoder<EventRecord> DECODER =
        new BinaryMessageDecoder<EventRecord>(MODEL, SCHEMA);

    public static BinaryMessageEncoder<EventRecord> getEncoder() {
      return ENCODER;
    }

    public static BinaryMessageDecoder<EventRecord> getDecoder() {
      return DECODER;
    }

    public static BinaryMessageDecoder<EventRecord> createDecoder(SchemaStore resolver) {
      return new BinaryMessageDecoder<EventRecord>(MODEL, SCHEMA, resolver);
    }

    public java.nio.ByteBuffer toByteBuffer()
        throws java.io.IOException {
      return ENCODER.encode(this);
    }

    public static EventRecord fromByteBuffer(java.nio.ByteBuffer b)
        throws java.io.IOException {
      return DECODER.decode(b);
    }

    private FixedSchema _fixedData;

    public EventRecord() {
    }

    public EventRecord(FixedSchema fixedData) {
      _fixedData = fixedData;
    }

    public org.apache.avro.specific.SpecificData getSpecificData() {
      return MODEL;
    }

    public org.apache.avro.Schema getSchema() {
      return SCHEMA;
    }

    // Used by DatumWriter.  Applications should not call.
    public java.lang.Object get(int field) {
      switch (field) {
        case 0:
          return _fixedData;
        default:
          throw new org.apache.avro.AvroRuntimeException("Bad index");
      }
    }

    // Used by DatumReader.  Applications should not call.
    @SuppressWarnings(value = "unchecked")
    public void put(int field, java.lang.Object value) {
      switch (field) {
        case 0:
          _fixedData = (FixedSchema) value;
          break;
        default:
          throw new org.apache.avro.AvroRuntimeException("Bad index");
      }
    }

    public FixedSchema getFixedData() {
      return _fixedData;
    }

    public void setFixedData(FixedSchema value) {
      _fixedData = value;
    }

    @SuppressWarnings("unchecked")
    private static final org.apache.avro.io.DatumWriter<EventRecord> WRITER =
        (org.apache.avro.io.DatumWriter<EventRecord>) MODEL.createDatumWriter(SCHEMA);

    @Override
    public void writeExternal(java.io.ObjectOutput out)
        throws java.io.IOException {
      WRITER.write(this, SpecificData.getEncoder(out));
    }

    @SuppressWarnings("unchecked")
    private static final org.apache.avro.io.DatumReader<EventRecord> READER =
        (org.apache.avro.io.DatumReader<EventRecord>) MODEL.createDatumReader(SCHEMA);

    @Override
    public void readExternal(java.io.ObjectInput in)
        throws java.io.IOException {
      READER.read(this, SpecificData.getDecoder(in));
    }

    @Override
    protected boolean hasCustomCoders() {
      return true;
    }

    @Override
    public void customEncode(org.apache.avro.io.Encoder out)
        throws java.io.IOException {
      out.writeFixed(_fixedData.bytes(), 0, 4);
    }

    @Override
    public void customDecode(org.apache.avro.io.ResolvingDecoder in)
        throws java.io.IOException {
      org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
      if (fieldOrder == null) {
        if (_fixedData == null) {
          _fixedData = new FixedSchema();
        }
        in.readFixed(_fixedData.bytes(), 0, 4);
      } else {
        for (int i = 0; i < 1; i++) {
          switch (fieldOrder[i].pos()) {
            case 0:
              if (_fixedData == null) {
                _fixedData = new FixedSchema();
              }
              in.readFixed(_fixedData.bytes(), 0, 4);
              break;

            default:
              throw new java.io.IOException("Corrupt ResolvingDecoder.");
          }
        }
      }
    }
  }

  /**
   * SpecificFixed created for testing Fixed data type
   */
  static class FixedSchema extends org.apache.avro.specific.SpecificFixed {
    private static final long serialVersionUID = -1121289150751596161L;
    public static final org.apache.avro.Schema SCHEMA = new org.apache.avro.Schema.Parser().parse(
        "{\"type\":\"fixed\",\"name\":\"FixedSchema\",\"doc\":\"\",\"size\":4}");

    public static org.apache.avro.Schema getClassSchema() {
      return SCHEMA;
    }

    public org.apache.avro.Schema getSchema() {
      return SCHEMA;
    }

    /** Creates a new FixedSchema */
    public FixedSchema() {
      super();
    }

    /**
     * Creates a new FixedSchema with the given bytes.
     * @param bytes The bytes to create the new FixedSchema.
     */
    public FixedSchema(byte[] bytes) {
      super(bytes);
    }

    private static final org.apache.avro.io.DatumWriter<FixedSchema> WRITER =
        new org.apache.avro.specific.SpecificDatumWriter<FixedSchema>(SCHEMA);

    @Override
    public void writeExternal(java.io.ObjectOutput out)
        throws java.io.IOException {
      WRITER.write(this, org.apache.avro.specific.SpecificData.getEncoder(out));
    }

    private static final org.apache.avro.io.DatumReader<FixedSchema> READER =
        new org.apache.avro.specific.SpecificDatumReader<FixedSchema>(SCHEMA);

    @Override
    public void readExternal(java.io.ObjectInput in)
        throws java.io.IOException {
      READER.read(this, org.apache.avro.specific.SpecificData.getDecoder(in));
    }
  }
}
