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
package org.apache.pinot.plugin.inputformat.csv;

import java.io.IOException;
import java.io.StringReader;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// Tests [CSVRecordExtractor] — see its class Javadoc for the cell-handling contract. Reader-level concerns
/// (header parsing, escapes, alternate delimiters) belong in [CSVRecordReaderTest], not here.
public class CSVRecordExtractorTest {

  // === Single value — every cell is a String ===

  @Test
  public void testStringPreserved()
      throws IOException {
    Object result = extract("a", "hello", "a", null);
    assertEquals(result, "hello");
  }

  @Test
  public void testNumericLookingCellStaysString()
      throws IOException {
    // CSV does not parse numbers — `42` is a String, not an Integer.
    Object result = extract("a", "42", "a", null);
    assertEquals(result, "42");
  }

  @Test
  public void testBooleanLookingCellStaysString()
      throws IOException {
    Object result = extract("a", "true", "a", null);
    assertEquals(result, "true");
  }

  // === Empty / unset cell → null ===

  @Test
  public void testEmptyCellReturnsNull()
      throws IOException {
    // `,,` between commas — empty cell.
    assertNull(extract("a,b,c", "x,,z", "b", null));
  }

  @Test
  public void testQuotedEmptyCellReturnsNull()
      throws IOException {
    // `""` is parsed as the empty string by Apache Commons CSV; the extractor treats both the same as null.
    assertNull(extract("a,b,c", "x,\"\",z", "b", null));
  }

  // === Multi-value delimiter ===

  @Test
  public void testMultiValueDelimiterSplitsToArray()
      throws IOException {
    Object[] result = (Object[]) extract("a", "x;y;z", "a", ';');
    assertEquals(result, new Object[]{"x", "y", "z"});
  }

  @Test
  public void testMultiValueSingleElementCollapsesToString()
      throws IOException {
    // CSV cannot distinguish between a multi-value column with one entry and a single-value column —
    // a single element after splitting is returned as a bare String, not wrapped in an array.
    Object result = extract("a", "x", "a", ';');
    assertEquals(result, "x");
  }

  @Test
  public void testNoMultiValueDelimiterKeepsRawString()
      throws IOException {
    // Without a configured multi-value delimiter, the cell is returned verbatim even if it contains the
    // would-be delimiter character.
    Object result = extract("a", "x;y;z", "a", null);
    assertEquals(result, "x;y;z");
  }

  // === Helpers ===

  /// Build a single-row [CSVRecord] from `header` and `row` (comma-separated), then extract `column` with
  /// optional multi-value delimiter.
  private static Object extract(String header, String row, String column, @Nullable Character mvDelimiter)
      throws IOException {
    CSVRecord record;
    try (CSVParser parser = CSVFormat.Builder.create()
        .setHeader(header.split(","))
        .build()
        .parse(new StringReader(row))) {
      record = parser.iterator().next();
    }
    CSVRecordExtractorConfig config = new CSVRecordExtractorConfig();
    config.setColumnNames(Set.of(header.split(",")));
    config.setMultiValueDelimiter(mvDelimiter);
    CSVRecordExtractor extractor = new CSVRecordExtractor();
    extractor.init(null, config);
    GenericRow gen = new GenericRow();
    extractor.extract(record, gen);
    return gen.getValue(column);
  }
}
