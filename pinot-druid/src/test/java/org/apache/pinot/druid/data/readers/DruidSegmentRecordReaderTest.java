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
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.recordtransformer.CompositeTransformer;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DruidSegmentRecordReaderTest {
  private static int wikipedia_size = 39244;
  private static File wikipedia_test;
  private static Schema wikipedia_test_schema;

  private static Schema csv_test_schema;
  private static File csv_test;

  private static File all_types_test;
  private static Schema all_types_test_schema;

  private File createFileFromFilename(String filename) {
    return new File(Preconditions
        .checkNotNull(DruidSegmentRecordReaderTest.class.getClassLoader().getResource(filename))
        .getFile());
  }

  private Schema createSchemaFromFile(File schemaFile)
      throws IOException {
    return Schema.fromFile(schemaFile);
  }

  private Schema createSchemaFromFilename(String filename)
      throws IOException {
    return createSchemaFromFile(createFileFromFilename(filename));
  }

  private void testReadRowsWithRecordList(Object[][] records, String[] columns, File dataFile, Schema schema)
      throws IOException {
    DruidSegmentRecordReader testReader = new DruidSegmentRecordReader(dataFile, schema);
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
                "In row " + row + " and column " + col + ", type of value in segment (" + transformedRow.getValue(columns[col]).getClass()
                    + ") does not match type of value in test record (" + records[row][col].getClass() + ").");
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
    Assert.assertEquals(genericRows.size(), records.length, "Size of GenericRows (" + genericRows.size()
        + ") must be equal to records size (" + records.length + ").");
  }

  private boolean compareMultiValueColumn(Object value1, Object value2) {
    Object[] value1Array = (Object[]) value1;
    Object[] value2Array = (Object[]) value2;
    Set<Object> value1Set = new HashSet<>(Arrays.asList(value1Array));
    Set<Object> value2Set = new HashSet<>(Arrays.asList(value2Array));
    return value1Set.containsAll(value2Set);
  }

  @BeforeClass
  public void setUp()
      throws IOException {
    // Wikipedia is sample data from Druid's Quickstart tutorial
    wikipedia_test = createFileFromFilename("test_druid_wikipedia");
    wikipedia_test_schema = createSchemaFromFilename("wikipedia-schema.json");

    csv_test = createFileFromFilename("test_druid_from_csv");
    csv_test_schema = createSchemaFromFilename("csv-test-schema.json");

    all_types_test = createFileFromFilename("test-druid-all-types");
    all_types_test_schema = createSchemaFromFilename("test-druid-all-types-schema.json");
  }

  @Test
  public void testReadAllRows()
      throws IOException {
    DruidSegmentRecordReader testReader;
    testReader = new DruidSegmentRecordReader(wikipedia_test, wikipedia_test_schema);

    List<GenericRow> genericRows = new ArrayList<>();
    while (testReader.hasNext()) {
      GenericRow nextRow = testReader.next();
      genericRows.add(nextRow);
    }
    Assert.assertEquals(genericRows.size(), wikipedia_size, "Generic row size must be " + wikipedia_size + ".");
  }

  @Test
  public void testCSVRecord()
      throws IOException {
    final String[] columms = {"sum_INT_SV", "INT_MV", "__time"};
    final Object[][] records = {
        {new Long(5), new String[]{"10", "15", "20"}, Instant.parse("2010-01-01T00:00:00.000Z").toEpochMilli()},
        {new Long(25), new String[]{"30", "35", "40"}, Instant.parse("2010-01-02T00:00:00.000Z").toEpochMilli()},
        {null, null, Instant.parse("2010-01-03T00:00:00.000Z").toEpochMilli()}};
    testReadRowsWithRecordList(records, columms, csv_test, csv_test_schema);
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

    testReadRowsWithRecordList(records, columns, all_types_test, all_types_test_schema);
  }
}
