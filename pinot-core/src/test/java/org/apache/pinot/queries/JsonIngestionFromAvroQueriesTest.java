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

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordReader;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.avro.Schema.*;


/**
 * Test if ComplexType (RECORD, ARRAY, MAP, UNION, ENUM, and FIXED) field from an AVRO file can be ingested into a JSON
 * column in a Pinot segment.
 */
public class JsonIngestionFromAvroQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "JsonIngestionFromAvroTest");
  private static final File AVRO_DATA_FILE = new File(INDEX_DIR, "JsonIngestionFromAvroTest.avro");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String INT_COLUMN = "intColumn";
  private static final String JSON_COLUMN_1 = "jsonColumn1"; // for testing RECORD, ARRAY, MAP, UNION
  private static final String JSON_COLUMN_2 = "jsonColumn2"; // for testing ENUM
  private static final String JSON_COLUMN_3 = "jsonColumn3"; // for testing FIXED
  private static final String STRING_COLUMN = "stringColumn";
  private static final org.apache.pinot.spi.data.Schema SCHEMA =
      new org.apache.pinot.spi.data.Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT)
          .addSingleValueDimension(JSON_COLUMN_1, FieldSpec.DataType.JSON)
          .addSingleValueDimension(JSON_COLUMN_2, FieldSpec.DataType.JSON)
          .addSingleValueDimension(JSON_COLUMN_3, FieldSpec.DataType.JSON)
          .addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING).build();
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

  /** @return {@link GenericRow} representing a row in Pinot table. */
  private static GenericRow createTableRecord(int intValue, String stringValue, Object jsonValue,
      GenericData.EnumSymbol enumValue, GenericData.Fixed fixedValue) {
    GenericRow record = new GenericRow();
    record.putValue(INT_COLUMN, intValue);
    record.putValue(STRING_COLUMN, stringValue);
    record.putValue(JSON_COLUMN_1, jsonValue);
    record.putValue(JSON_COLUMN_2, enumValue);
    record.putValue(JSON_COLUMN_3, fixedValue);
    return record;
  }

  private static Map<String, String> createMapField(Pair<String, String>[] pairs) {
    Map<String, String> map = new HashMap<>();
    for (Pair<String, String> pair : pairs) {
      map.put(pair.getLeft(), pair.getRight());
    }
    return map;
  }

  private static Schema createRecordSchema() {
    List<Field> fields = new ArrayList<>();
    fields.add(new Field("id", create(Type.INT)));
    fields.add(new Field("name", create(Type.STRING)));
    return createRecord("record", "doc", JsonIngestionFromAvroQueriesTest.class.getCanonicalName(), false, fields);
  }

  private static GenericData.Record createRecordField(String k1, int v1, String k2, String v2) {
    GenericData.Record record = new GenericData.Record(createRecordSchema());
    record.put(k1, v1);
    record.put(k2, v2);
    return record;
  }

  private static GenericData.EnumSymbol createEnumField(Schema enumSchema, String enumValue) {
    return new GenericData.EnumSymbol(enumSchema, enumValue);
  }

  private static GenericData.Fixed createFixedField(Schema fixedSchema, int value) {
    byte[] bytes = {(byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value};
    return new GenericData.Fixed(fixedSchema, bytes);
  }

  private static void createInputFile()
      throws IOException {
    INDEX_DIR.mkdir();
    Schema avroSchema = createRecord("eventsRecord", null, null, false);
    Schema enumSchema = createEnum("direction", null, null, Arrays.asList("UP", "DOWN", "LEFT", "RIGHT"));
    Schema fixedSchema = createFixed("fixed", null, null, 4);
    List<Field> fields = Arrays.asList(
        new Field(INT_COLUMN, createUnion(Lists.newArrayList(create(Type.INT), create(Type.NULL))), null, null),
        new Field(STRING_COLUMN, createUnion(Lists.newArrayList(create(Type.STRING), create(Type.NULL))), null, null),
        new Field(JSON_COLUMN_1,
            createUnion(createArray(create(Type.STRING)), createMap(create(Type.STRING)), createRecordSchema(),
                create(Type.STRING), create(Type.NULL))), new Field(JSON_COLUMN_2, enumSchema),
        new Field(JSON_COLUMN_3, fixedSchema));
    avroSchema.setFields(fields);
    List<GenericRow> inputRecords = new ArrayList<>();
    // Insert ARRAY
    inputRecords.add(
        createTableRecord(1, "daffy duck", Arrays.asList("this", "is", "a", "test"), createEnumField(enumSchema, "UP"),
            createFixedField(fixedSchema, 1)));

    // Insert MAP
    inputRecords.add(
        createTableRecord(2, "mickey mouse", createMapField(new Pair[]{Pair.of("a", "1"), Pair.of("b", "2")}),
            createEnumField(enumSchema, "DOWN"), createFixedField(fixedSchema, 2)));
    inputRecords.add(
        createTableRecord(3, "donald duck", createMapField(new Pair[]{Pair.of("a", "1"), Pair.of("b", "2")}),
            createEnumField(enumSchema, "UP"), createFixedField(fixedSchema, 3)));
    inputRecords.add(
        createTableRecord(4, "scrooge mcduck", createMapField(new Pair[]{Pair.of("a", "1"), Pair.of("b", "2")}),
            createEnumField(enumSchema, "LEFT"), createFixedField(fixedSchema, 4)));

    // insert RECORD
    inputRecords.add(createTableRecord(5, "minney mouse", createRecordField("id", 1, "name", "minney"),
        createEnumField(enumSchema, "RIGHT"), createFixedField(fixedSchema, 5)));

    // Insert simple Java String (gets converted into JSON value)
    inputRecords.add(
        createTableRecord(6, "pluto", "test", createEnumField(enumSchema, "DOWN"), createFixedField(fixedSchema, 6)));

    // Insert JSON string (gets converted into JSON document)
    inputRecords.add(
        createTableRecord(7, "scooby doo", "{\"name\":\"scooby\",\"id\":7}", createEnumField(enumSchema, "UP"),
            createFixedField(fixedSchema, 7)));

    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, AVRO_DATA_FILE);
      for (GenericRow inputRecord : inputRecords) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(INT_COLUMN, inputRecord.getValue(INT_COLUMN));
        record.put(STRING_COLUMN, inputRecord.getValue(STRING_COLUMN));
        record.put(JSON_COLUMN_1, inputRecord.getValue(JSON_COLUMN_1));
        record.put(JSON_COLUMN_2, inputRecord.getValue(JSON_COLUMN_2));
        record.put(JSON_COLUMN_3, inputRecord.getValue(JSON_COLUMN_3));
        fileWriter.append(record);
      }
    }
  }

  private static RecordReader createRecordReader()
      throws IOException {
    Set<String> set = new HashSet<>();
    set.add(INT_COLUMN);
    set.add(STRING_COLUMN);
    set.add(JSON_COLUMN_1);
    set.add(JSON_COLUMN_2);
    set.add(JSON_COLUMN_3);
    AvroRecordReader avroRecordReader = new AvroRecordReader();
    avroRecordReader.init(AVRO_DATA_FILE, set, null);
    return avroRecordReader;
  }

  /** Create an AVRO file and then ingest it into Pinot while creating a JsonIndex. */
  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    createInputFile();

    List<String> jsonIndexColumns = Arrays.asList(JSON_COLUMN_1, JSON_COLUMN_2, JSON_COLUMN_3);
    TABLE_CONFIG.getIndexingConfig().setJsonIndexColumns(jsonIndexColumns);
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());
    segmentGeneratorConfig.setInputFilePath(AVRO_DATA_FILE.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, createRecordReader());
    driver.build();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setTableConfig(TABLE_CONFIG);
    indexLoadingConfig.setJsonIndexColumns(new HashSet<String>(jsonIndexColumns));
    indexLoadingConfig.setReadMode(ReadMode.mmap);

    ImmutableSegment immutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  /** Verify that we can query the JSON column that ingested ComplexType data from an AVRO file (see setUp). */
  @Test
  public void testSimpleSelectOnJsonColumn() {
    Operator<SelectionResultsBlock> operator =
        getOperator("select intColumn, stringColumn, jsonColumn1, jsonColumn2 FROM " + "testTable limit 100");
    SelectionResultsBlock block = operator.nextBlock();
    Collection<Object[]> rows = block.getRows();
    Assert.assertEquals(block.getDataSchema().getColumnDataType(0), DataSchema.ColumnDataType.INT);
    Assert.assertEquals(block.getDataSchema().getColumnDataType(1), DataSchema.ColumnDataType.STRING);
    Assert.assertEquals(block.getDataSchema().getColumnDataType(2), DataSchema.ColumnDataType.JSON);

    List<String> expecteds = Arrays.asList("[1, daffy duck, [\"this\",\"is\",\"a\",\"test\"], \"UP\"]",
        "[2, mickey mouse, {\"a\":\"1\",\"b\":\"2\"}, \"DOWN\"]", "[3, donald duck, {\"a\":\"1\",\"b\":\"2\"}, \"UP\"]",
        "[4, scrooge mcduck, {\"a\":\"1\",\"b\":\"2\"}, \"LEFT\"]",
        "[5, minney mouse, {\"name\":\"minney\",\"id\":1}, \"RIGHT\"]", "[6, pluto, \"test\", \"DOWN\"]",
        "[7, scooby doo, {\"name\":\"scooby\",\"id\":7}, \"UP\"]");

    int index = 0;
    Iterator<Object[]> iterator = rows.iterator();
    while (iterator.hasNext()) {
      Object[] row = iterator.next();
      Assert.assertEquals(Arrays.toString(row), expecteds.get(index++));
    }
  }

  /** Verify simple path expression query on ingested Avro file. */
  @Test
  public void testJsonPathSelectOnJsonColumn() {
    Operator<SelectionResultsBlock> operator = getOperator(
        "select intColumn, json_extract_scalar(jsonColumn1, '$.name', " + "'STRING', 'null') FROM testTable");
    SelectionResultsBlock block = operator.nextBlock();
    Collection<Object[]> rows = block.getRows();
    Assert.assertEquals(block.getDataSchema().getColumnDataType(0), DataSchema.ColumnDataType.INT);
    Assert.assertEquals(block.getDataSchema().getColumnDataType(1), DataSchema.ColumnDataType.STRING);

    List<String> expecteds =
        Arrays.asList("[1, null]", "[2, null]", "[3, null]", "[4, null]", "[5, minney]", "[6, null]", "[7, scooby]");
    int index = 0;

    Iterator<Object[]> iterator = rows.iterator();
    while (iterator.hasNext()) {
      Object[] row = iterator.next();
      Assert.assertEquals(Arrays.toString(row), expecteds.get(index++));
    }
  }

  /** Verify simple path expression query on ingested Avro file. */
  @Test
  public void testStringValueSelectOnJsonColumn() {
    Operator<SelectionResultsBlock> operator = getOperator(
        "SELECT json_extract_scalar(jsonColumn1, '$', 'STRING') FROM "
            + "testTable WHERE JSON_MATCH(jsonColumn1, '\"$\" = ''test''')");
    SelectionResultsBlock block = operator.nextBlock();
    Collection<Object[]> rows = block.getRows();
    Assert.assertEquals(block.getDataSchema().getColumnDataType(0), DataSchema.ColumnDataType.STRING);

    List<String> expecteds = Arrays.asList("[test]");
    int index = 0;

    Iterator<Object[]> iterator = rows.iterator();
    while (iterator.hasNext()) {
      Object[] row = iterator.next();
      Assert.assertEquals(Arrays.toString(row), expecteds.get(index++));
    }
  }

  /** Verify that ingestion from avro FIXED type field (jsonColumn3) to Pinot JSON column worked fine. */
  @Test
  public void testSimpleSelectOnFixedJsonColumn() {
    Operator<SelectionResultsBlock> operator = getOperator("select jsonColumn3 FROM testTable");
    SelectionResultsBlock block = operator.nextBlock();
    Collection<Object[]> rows = block.getRows();
    Assert.assertEquals(block.getDataSchema().getColumnDataType(0), DataSchema.ColumnDataType.JSON);

    List<String> expecteds =
        Arrays.asList("[[0,0,0,1]]", "[[0,0,0,2]]", "[[0,0,0,3]]", "[[0,0,0,4]]", "[[0,0,0,5]]", "[[0,0,0,6]]",
            "[[0,0,0,7]]");
    int index = 0;

    Iterator<Object[]> iterator = rows.iterator();
    while (iterator.hasNext()) {
      Object[] row = iterator.next();
      Assert.assertEquals(Arrays.toString(row), expecteds.get(index++));
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
