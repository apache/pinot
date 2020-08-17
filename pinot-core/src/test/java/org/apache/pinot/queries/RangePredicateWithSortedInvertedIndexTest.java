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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.common.utils.Pairs;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.SelectionOnlyOperator;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class RangePredicateWithSortedInvertedIndexTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "RangePredicateWithSortedInvertedIndexTest");
  private static final String TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String D1 = "STRING_COL";
  private static final String M1 = "INT_COL"; // sorted column
  private static final String M2 = "LONG_COL";

  private static final int NUM_ROWS = 30000;
  private static final int INT_BASE_VALUE = 0;

  private static final long RANDOM_SEED = System.nanoTime();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final String ERROR_MESSAGE = "Random seed: " + RANDOM_SEED;

  private final String[] _stringValues = new String[NUM_ROWS];
  private final long[] _longValues = new long[NUM_ROWS];

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

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    buildSegment();
    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private void buildSegment()
      throws Exception {
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int rowIndex = 0; rowIndex < NUM_ROWS; rowIndex++) {
      GenericRow row = new GenericRow();
      _stringValues[rowIndex] = RandomStringUtils.randomAlphanumeric(10);
      row.putValue(D1, _stringValues[rowIndex]);
      row.putValue(M1, INT_BASE_VALUE + rowIndex);
      _longValues[rowIndex] = RANDOM.nextLong();
      row.putValue(M2, _longValues[rowIndex]);
      rows.add(row);
    }

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(TABLE_NAME).addSingleValueDimension(D1, FieldSpec.DataType.STRING)
            .addMetric(M1, FieldSpec.DataType.INT).addMetric(M2, FieldSpec.DataType.LONG).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  @Test
  public void testInnerSegmentQuery() {
    String query = "SELECT STRING_COL, INT_COL FROM testTable WHERE INT_COL >= 20000 LIMIT 100000";
    Pairs.IntPair pair = new Pairs.IntPair(20000, 29999);
    runQuery(query, 10000, Lists.newArrayList(pair), 2);

    query = "SELECT STRING_COL, INT_COL FROM testTable WHERE INT_COL >= 20000 AND INT_COL <= 23666 LIMIT 100000";
    pair = new Pairs.IntPair(20000, 23666);
    runQuery(query, 3667, Lists.newArrayList(pair), 2);

    query = "SELECT STRING_COL, INT_COL FROM testTable WHERE INT_COL <= 20000 LIMIT 100000";
    pair = new Pairs.IntPair(0, 20000);
    runQuery(query, 20001, Lists.newArrayList(pair), 2);

    String filter = "WHERE (INT_COL >= 15000 AND INT_COL <= 16665) OR (INT_COL >= 18000 AND INT_COL <= 19887)";
    query = "SELECT STRING_COL, INT_COL FROM testTable " + filter + " LIMIT 100000";
    pair = new Pairs.IntPair(15000, 16665);
    Pairs.IntPair pair1 = new Pairs.IntPair(18000, 19987);
    runQuery(query, 3554, Lists.newArrayList(pair, pair1), 2);

    // range predicate on sorted column which will use sorted inverted index based iterator
    // along with range predicate on unsorted column that uses scan based iterator
    int index = RANDOM.nextInt(NUM_ROWS);
    long longPredicateValue = _longValues[index];
    int count = 0;
    List<Pairs.IntPair> pairs = new ArrayList<>();
    Pairs.IntPair current = null;
    for (int i = 0; i < _longValues.length; i++) {
      if (_longValues[i] >= longPredicateValue && i >= 15000 && i <= 16665) {
        if (current == null) {
          current = new Pairs.IntPair(i, i);
        } else {
          if (i == current.getRight() + 1) {
            current.setRight(i);
          } else {
            if (i <= _longValues.length - 2) {
              pairs.add(current);
              current = new Pairs.IntPair(i, i);
            }
          }
        }
        count++;
      }
    }
    pairs.add(current);
    filter = "WHERE INT_COL >= 15000 AND INT_COL <= 16665 AND LONG_COL >= " + longPredicateValue;
    query = "SELECT STRING_COL, INT_COL, LONG_COL FROM testTable " + filter + " LIMIT 100000";
    runQuery(query, count, pairs, 3);

    // empty resultset
    query = "SELECT STRING_COL, INT_COL FROM testTable WHERe INT_COL < 0 LIMIT 100000";
    runQuery(query, 0, null, 0);

    // empty resultset
    query = "SELECT STRING_COL, INT_COL FROM testTable WHERE INT_COL > 30000 LIMIT 100000";
    runQuery(query, 0, null, 0);
  }

  private void runQuery(String query, int count, List<Pairs.IntPair> intPairs, int numColumns) {
    SelectionOnlyOperator operator = getOperatorForPqlQuery(query);
    IntermediateResultsBlock block = operator.nextBlock();
    Collection<Object[]> rows = block.getSelectionResult();
    assertNotNull(rows, ERROR_MESSAGE);
    assertEquals(rows.size(), count, ERROR_MESSAGE);
    if (count > 0) {
      Pairs.IntPair pair = intPairs.get(0);
      int startPos = pair.getLeft();
      int pairPos = 0;
      for (Object[] row : rows) {
        assertEquals(numColumns, row.length, ERROR_MESSAGE);
        assertEquals(row[0], _stringValues[startPos], ERROR_MESSAGE);
        assertEquals(row[1], startPos, ERROR_MESSAGE);
        if (numColumns == 3) {
          assertEquals(row[2], _longValues[startPos], ERROR_MESSAGE);
        }
        startPos++;
        if (startPos > pair.getRight() && pairPos <= intPairs.size() - 2) {
          pairPos++;
          pair = intPairs.get(pairPos);
          startPos = pair.getLeft();
        }
      }
    }
  }
}
