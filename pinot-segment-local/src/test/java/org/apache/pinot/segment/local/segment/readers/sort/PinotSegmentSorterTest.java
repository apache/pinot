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
package org.apache.pinot.segment.local.segment.readers.sort;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


/// Tests for {@link PinotSegmentSorter#getSortedDocIds}, covering both dictionary-encoded and no-dictionary columns
/// across all supported single-value stored types.
public class PinotSegmentSorterTest implements PinotBuffersAfterMethodCheckRule {

  private static final String SORT_COL = "sort_col";
  private static final String TIE_COL = "tie_col";
  private static final int NUM_ROWS = 20;

  private static final File TMP_DIR =
      new File(FileUtils.getTempDirectory(), PinotSegmentSorterTest.class.getName());

  @DataProvider
  public static Object[][] columnParams() {
    DataType[] dataTypes = {
        DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE,
        DataType.BIG_DECIMAL, DataType.STRING, DataType.BYTES,
    };
    List<Object[]> params = new ArrayList<>();
    for (DataType dataType : dataTypes) {
      params.add(new Object[]{dataType, true});   // dictionary-encoded
      params.add(new Object[]{dataType, false});  // no-dictionary
    }
    return params.toArray(new Object[0][]);
  }

  /// Verifies that sorting a shuffled segment on a single column produces documents in ascending value order,
  /// for every supported stored type and both dictionary-encoded and no-dictionary forward indexes.
  @Test(dataProvider = "columnParams")
  public void testSortBySingleColumn(DataType dataType, boolean dictionaryEncoded)
      throws Exception {
    File outDir = new File(TMP_DIR, "single_" + dataType + "_" + dictionaryEncoded);

    List<String> noDictColumns = dictionaryEncoded ? List.of() : List.of(SORT_COL);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .setNoDictionaryColumns(noDictColumns)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(SORT_COL, dataType)
        .build();

    // Build shuffled rows: values are i=0..NUM_ROWS-1 in shuffled order
    List<Integer> indices = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      indices.add(i);
    }
    Collections.shuffle(indices);

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(SORT_COL, value(dataType, indices.get(i)));
      rows.add(row);
    }

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(outDir.getAbsolutePath());
    config.setSegmentName("testSegment");

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    ImmutableSegment segment = ImmutableSegmentLoader.load(driver.getOutputDirectory(), ReadMode.mmap);
    try (PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(segment, SORT_COL)) {
      Map<String, PinotSegmentColumnReader> readerMap = new HashMap<>();
      readerMap.put(SORT_COL, columnReader);

      int[] sortedDocIds = new PinotSegmentSorter(NUM_ROWS, readerMap).getSortedDocIds(List.of(SORT_COL));

      assertAscending(sortedDocIds, rows, SORT_COL, dataType);
    } finally {
      segment.destroy();
    }
  }

  /// Verifies that a two-column sort breaks ties on the primary column using the secondary column.
  /// Uses no-dictionary columns for both to exercise the new code path.
  @Test
  public void testSortByMultipleColumns()
      throws Exception {
    File outDir = new File(TMP_DIR, "multi_col");

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .setNoDictionaryColumns(List.of(SORT_COL, TIE_COL))
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(SORT_COL, DataType.INT)
        .addSingleValueDimension(TIE_COL, DataType.INT)
        .build();

    // primary has only 3 distinct values; tie_col breaks ties
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(SORT_COL, i % 3);       // primary: 0, 1, 2, 0, 1, 2, ...
      row.putValue(TIE_COL, NUM_ROWS - i); // secondary: descending, so sort must reverse it
      rows.add(row);
    }
    // Shuffle so the sorter actually has work to do
    Collections.shuffle(rows);

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(outDir.getAbsolutePath());
    config.setSegmentName("testSegment");

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    ImmutableSegment segment = ImmutableSegmentLoader.load(driver.getOutputDirectory(), ReadMode.mmap);
    try (PinotSegmentColumnReader sortColReader = new PinotSegmentColumnReader(segment, SORT_COL);
        PinotSegmentColumnReader tieColReader = new PinotSegmentColumnReader(segment, TIE_COL)) {
      Map<String, PinotSegmentColumnReader> readerMap = new HashMap<>();
      readerMap.put(SORT_COL, sortColReader);
      readerMap.put(TIE_COL, tieColReader);

      int[] sortedDocIds =
          new PinotSegmentSorter(NUM_ROWS, readerMap).getSortedDocIds(List.of(SORT_COL, TIE_COL));

      // Verify primary then secondary ordering
      for (int i = 1; i < NUM_ROWS; i++) {
        int primary1 = (Integer) rows.get(sortedDocIds[i - 1]).getValue(SORT_COL);
        int primary2 = (Integer) rows.get(sortedDocIds[i]).getValue(SORT_COL);
        int tie1 = (Integer) rows.get(sortedDocIds[i - 1]).getValue(TIE_COL);
        int tie2 = (Integer) rows.get(sortedDocIds[i]).getValue(TIE_COL);

        assertTrue(primary1 < primary2 || (primary1 == primary2 && tie1 <= tie2),
            "Multi-column sort order violated at position " + i);
      }
    } finally {
      segment.destroy();
    }
  }

  /// Returns the i-th ascending value for the given data type.
  private static Object value(DataType dataType, int i) {
    switch (dataType) {
      case INT:
        return i;
      case LONG:
        return (long) i;
      case FLOAT:
        return (float) i;
      case DOUBLE:
        return (double) i;
      case BIG_DECIMAL:
        return new BigDecimal(i);
      case STRING:
        return String.format("%05d", i);
      case BYTES:
        return new byte[]{(byte) (i >> 8), (byte) i};
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  /// Asserts that the values at {@code sortedDocIds} are in non-decreasing order.
  private static void assertAscending(int[] sortedDocIds, List<GenericRow> rows, String column, DataType dataType) {
    for (int i = 1; i < sortedDocIds.length; i++) {
      Object v1 = rows.get(sortedDocIds[i - 1]).getValue(column);
      Object v2 = rows.get(sortedDocIds[i]).getValue(column);
      int cmp;
      switch (dataType) {
        case INT:
          cmp = Integer.compare((Integer) v1, (Integer) v2);
          break;
        case LONG:
          cmp = Long.compare((Long) v1, (Long) v2);
          break;
        case FLOAT:
          cmp = Float.compare((Float) v1, (Float) v2);
          break;
        case DOUBLE:
          cmp = Double.compare((Double) v1, (Double) v2);
          break;
        case BIG_DECIMAL:
          cmp = ((BigDecimal) v1).compareTo((BigDecimal) v2);
          break;
        case STRING:
          cmp = ((String) v1).compareTo((String) v2);
          break;
        case BYTES:
          cmp = ByteArray.compare((byte[]) v1, (byte[]) v2);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported data type: " + dataType);
      }
      assertTrue(cmp <= 0,
          "Sort order violated at position " + i + " for type " + dataType + ": " + v1 + " > " + v2);
    }
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(TMP_DIR);
  }
}
