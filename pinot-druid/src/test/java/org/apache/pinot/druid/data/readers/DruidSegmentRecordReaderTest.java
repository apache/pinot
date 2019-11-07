package org.apache.pinot.druid.data.readers;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.readers.JSONRecordReader;
import org.apache.pinot.core.data.recordtransformer.CompositeTransformer;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class DruidSegmentRecordReaderTest {
  // test_sample_data is a Druid segment that was created from pinot-core/src/test/resources/data/test_sample_data.json
  private static final File TEST_SAMPLE_DATA = new File(Preconditions
      .checkNotNull(DruidSegmentRecordReaderTest.class.getClassLoader().getResource("test_sample_data"))
      .getFile());
  private static final File TEST_SAMPLE_DATA_JSON = new File(Preconditions
      .checkNotNull(DruidSegmentRecordReaderTest.class.getClassLoader().getResource("test_sample_data_dumped.json"))
      .getFile());
  // Same schema as in RecordReaderSampleDataTest
  private static final Schema TEST_SAMPLE_DATA_SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension("column1", FieldSpec.DataType.LONG)
        .addSingleValueDimension("column2", FieldSpec.DataType.LONG)
        .addSingleValueDimension("column3", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column7", FieldSpec.DataType.STRING)
        .addSingleValueDimension("unknown_dimension", FieldSpec.DataType.STRING)
        .addMetric("met_impressionCount", FieldSpec.DataType.LONG).addMetric("unknown_metric", FieldSpec.DataType.DOUBLE)
        .build();
  // Test based on CSVRecordReaderTest
  private static final File CSV_TEST = new File(Preconditions
      .checkNotNull(DruidSegmentRecordReaderTest.class.getClassLoader().getResource("test_druid_from_csv"))
      .getFile());
  private static final Schema CSV_TEST_SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension("__time", FieldSpec.DataType.LONG)
      .addMultiValueDimension("STR_MV", FieldSpec.DataType.STRING)
      .addMetric("INT_SV", FieldSpec.DataType.LONG)
      .build();
  private static final File ALL_TYPES_TEST = new File(Preconditions
      .checkNotNull(DruidSegmentRecordReaderTest.class.getClassLoader().getResource("test_druid_all_types"))
      .getFile());
  private static final Schema ALL_TYPES_TEST_SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension("__time", FieldSpec.DataType.LONG)
      .addSingleValueDimension("stringval", FieldSpec.DataType.STRING)
      .addMultiValueDimension("tags", FieldSpec.DataType.STRING)
      .addMetric("intnum", FieldSpec.DataType.LONG)
      .addMetric("floatnum", FieldSpec.DataType.FLOAT)
      .build();

  private void testReadRowsWithRecordList(Object[][] records, String[] columns, File dataFile, Schema schema)
      throws IOException {
    try (DruidSegmentRecordReader testReader = new DruidSegmentRecordReader(dataFile, schema)) {
      CompositeTransformer defaultTransformer = CompositeTransformer.getDefaultTransformer(schema);

      List<GenericRow> genericRows = new ArrayList<>();
      int row = 0;
      while (testReader.hasNext()) {
        Assert.assertTrue(row < records.length);

        GenericRow nextRow = testReader.next();
        GenericRow transformedRow = defaultTransformer.transform(nextRow);

        for (int col = 0; col < columns.length; col++) {
          if (records[row][col] != null) {
            if (transformedRow.getValue(columns[col]) != null && records[row][col] != null) {
              Assert.assertTrue(transformedRow.getValue(columns[col]).getClass() == records[row][col].getClass(),
                  String.format("In row {} and column {}, type of value in segment ({}) does not match type of value in test record ({}).",
                      row, col, transformedRow.getValue(columns[col]).getClass(), records[row][col].getClass()));
              if (transformedRow.getValue(columns[col]).getClass() == String[].class) {
                Assert.assertTrue(compareMultiValueColumn(transformedRow.getValue(columns[col]), records[row][col]));
              } else {
                Assert.assertEquals(transformedRow.getValue(columns[col]), records[row][col]);
              }
            }
          }
        }
        genericRows.add(nextRow);
        row += 1;
      }
      Assert.assertFalse(testReader.hasNext());
      Assert.assertEquals(genericRows.size(), records.length, String.format("Size of GenericRows ({}) must be equal to records size ({}).", genericRows.size(), records.length));
    }
  }

  private boolean compareMultiValueColumn(Object value1, Object value2) {
    Object[] value1Array = (Object[]) value1;
    Object[] value2Array = (Object[]) value2;
    Set<Object> value1Set = new HashSet<>(Arrays.asList(value1Array));
    Set<Object> value2Set = new HashSet<>(Arrays.asList(value2Array));
    return value1Set.containsAll(value2Set);
  }

  @Test
  public void testRecordReaderWithJson()
      throws IOException {
    CompositeTransformer defaultTransformer = CompositeTransformer.getDefaultTransformer(TEST_SAMPLE_DATA_SCHEMA);
    try (DruidSegmentRecordReader druidRecordReader = new DruidSegmentRecordReader(TEST_SAMPLE_DATA, TEST_SAMPLE_DATA_SCHEMA);
        JSONRecordReader jsonRecordReader = new JSONRecordReader(TEST_SAMPLE_DATA_JSON, TEST_SAMPLE_DATA_SCHEMA)) {
      int numRecords = 0;
      while (druidRecordReader.hasNext()) {
        assertTrue(jsonRecordReader.hasNext());
        numRecords++;

        GenericRow druidRecord = defaultTransformer.transform(druidRecordReader.next());
        GenericRow jsonRecord = defaultTransformer.transform(jsonRecordReader.next());
        assertEquals(druidRecord, jsonRecord);

        // Check the values from the first record
        if (numRecords == 1) {
          // Dimensions
          assertEquals(druidRecord.getValue("column1"), 1840748525967736008L);
          assertEquals(druidRecord.getValue("column2"), 231355578L);
          assertEquals(druidRecord.getValue("column3"), "CezOib");

          // Dimension empty string
          assertEquals(druidRecord.getValue("column7").toString(), "null");

          // Dimension default column
          assertEquals(druidRecord.getValue("unknown_dimension"), "null");

          // Metric
          assertEquals(druidRecord.getValue("met_impressionCount"), 8637957270245933828L);

          // Metric default column
          assertEquals(druidRecord.getValue("unknown_metric"), 0.0);
        }
      }
      assertEquals(numRecords, 10001);
    }
  }

  @Test
  public void testCSVRecord()
      throws IOException {
    final String[] columms = {"INT_SV", "STR_MV", "__time"};
    final Object[][] records = {
        {new Long(5), new String[]{"10", "15", "20"}, Instant.parse("2010-01-01T00:00:00.000Z").toEpochMilli()},
        {new Long(25), new String[]{"30", "35", "40"}, Instant.parse("2010-01-02T00:00:00.000Z").toEpochMilli()},
        {null, null, Instant.parse("2010-01-03T00:00:00.000Z").toEpochMilli()}};
    testReadRowsWithRecordList(records, columms, CSV_TEST, CSV_TEST_SCHEMA);
  }

  @Test
  public void testAllColumnTypes()
      throws IOException {
    final String[] columns = {"__time", "intnum", "floatnum", "tags", "stringval"};
    final Object[][] records = {
        {Long.parseLong("1294704000000"), Long.parseLong("0"), null, null, null},
        {Long.parseLong("1294790400000"), Long.parseLong("1"), new Float(0.1), new String[]{"t1","t2","t3"}, "test1"},
        {Long.parseLong("1294876800000"), Long.parseLong("2"), new Float(10.02), new String[]{"t3","t4","t5"}, "test2"},
        {Long.parseLong("1294963200000"), Long.parseLong("3"), new Float(100.003), new String[]{"t5","t6","t7"}, "test3"}};
    testReadRowsWithRecordList(records, columns, ALL_TYPES_TEST, ALL_TYPES_TEST_SCHEMA);
  }
}
