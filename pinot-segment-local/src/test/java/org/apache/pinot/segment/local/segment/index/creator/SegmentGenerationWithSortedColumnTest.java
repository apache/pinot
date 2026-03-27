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
package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterMethodCheckRule;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Tests that the `isSorted` metadata flag is correctly set when creating an immutable segment directly via
/// [SegmentIndexCreationDriverImpl], for both dictionary-encoded and no-dictionary sorted columns.
public class SegmentGenerationWithSortedColumnTest implements PinotBuffersAfterMethodCheckRule {

  private static final String SORTED_COLUMN = "sorted_col";
  private static final String STRING_COLUMN = "string_col";

  private static final File TMP_DIR =
      new File(FileUtils.getTempDirectory(), SegmentGenerationWithSortedColumnTest.class.getName());

  @DataProvider
  public static Object[][] sortedColumnParams() {
    DataType[] dataTypes = {
        DataType.INT,
        DataType.LONG,
        DataType.FLOAT,
        DataType.DOUBLE,
        DataType.BIG_DECIMAL,
        DataType.BOOLEAN,
        DataType.TIMESTAMP,
        DataType.STRING,
        DataType.BYTES,
    };
    List<Object[]> params = new ArrayList<>();
    for (DataType dataType : dataTypes) {
      params.add(new Object[]{dataType, true});   // dictionary-encoded
      params.add(new Object[]{dataType, false});  // no-dictionary
    }
    return params.toArray(new Object[0][]);
  }

  /// Verifies that when rows are fed to the segment creator already in sorted order on the designated
  /// sorted column, the resulting immutable segment has `isSorted=true` for that column and `isSorted=false`
  /// for a column that is not sorted. Covers all supported single-value data types, both dictionary-encoded
  /// and no-dictionary.
  @Test(dataProvider = "sortedColumnParams")
  public void testSortedColumnMetadata(DataType dataType, boolean dictionaryEncoded)
      throws Exception {
    File outDir = new File(TMP_DIR, "sorted_" + dataType + "_" + dictionaryEncoded + "_" + System.nanoTime());

    List<String> noDictColumns = dictionaryEncoded ? List.of() : List.of(SORTED_COLUMN);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("testTable")
        .setSortedColumn(SORTED_COLUMN)
        .setNoDictionaryColumns(noDictColumns)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(SORTED_COLUMN, dataType)
        .addSingleValueDimension(STRING_COLUMN, DataType.STRING)
        .build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(outDir.getAbsolutePath());
    config.setSegmentName("testSegment");

    // Rows in ascending order on SORTED_COLUMN.
    // STRING_COLUMN uses reverse values so it is descending (not sorted) in the same row order.
    int numRows = 10;
    List<GenericRow> rows = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      GenericRow row = new GenericRow();
      row.putValue(SORTED_COLUMN, sortedValue(dataType, i, numRows));
      row.putValue(STRING_COLUMN, "str" + (numRows - 1 - i));
      rows.add(row);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(driver.getOutputDirectory());

    ColumnMetadata sortedColMeta = segmentMetadata.getColumnMetadataFor(SORTED_COLUMN);
    assertTrue(sortedColMeta.isSorted(),
        "Sorted column must have isSorted=true (dataType=" + dataType + ", dictionaryEncoded=" + dictionaryEncoded
            + ")");
    assertEquals(sortedColMeta.hasDictionary(), dictionaryEncoded,
        "Dictionary encoding must match config (dataType=" + dataType + ", dictionaryEncoded=" + dictionaryEncoded
            + ")");

    ColumnMetadata nonSortedColMeta = segmentMetadata.getColumnMetadataFor(STRING_COLUMN);
    assertFalse(nonSortedColMeta.isSorted(), "Non-sorted column must have isSorted=false");
  }

  /// Returns the i-th ascending value for the given data type, out of numRows total rows.
  private static Object sortedValue(DataType dataType, int i, int numRows) {
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
      case BOOLEAN:
        // First half false (0), second half true (1) — yields a non-decreasing sequence
        return i >= numRows / 2 ? 1 : 0;
      case TIMESTAMP:
        return (long) i * 1000L;
      case STRING:
        // Zero-padded to preserve lexicographic order
        return String.format("%05d", i);
      case BYTES:
        return new byte[]{(byte) i};
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  @AfterMethod
  public void tearDownTest() {
    FileUtils.deleteQuietly(TMP_DIR);
  }
}
