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
package org.apache.pinot.core.startree;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.filter.predicate.BaseRawValueBasedPredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.EqualsPredicateEvaluatorFactory;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.plan.FilterPlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.startree.v2.builder.MultipleTreesBuilder;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Tests for [StarTreeUtils] verifying that star-tree can consume filters on columns configured with a `RAW` forward
/// index and a separated dictionary.
///
/// Before the fix, [org.apache.pinot.core.startree.operator.StarTreeFilterOperator] invoked `getMatchingDictIds()` on
/// a raw-value evaluator during tree traversal and threw `UnsupportedOperationException`. This test locks in that
/// the star-tree planner upgrades such evaluators to dictionary-based, yielding the same query behavior as if the
/// column were `DICTIONARY`-encoded.
public class StarTreeUtilsTest {

  private static final String COLUMN = "raw_dim_with_dict";

  //-------------------------------------------------------------------------
  // Unit tests for the private helper
  //-------------------------------------------------------------------------

  /// Raw-value evaluator on a column with a dictionary is rebuilt as dictionary-based.
  @Test
  public void testToDictionaryBasedConvertsRawEvaluator() {
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.length()).thenReturn(2);
    when(dictionary.getStringValue(0)).thenReturn("v0");
    when(dictionary.getStringValue(1)).thenReturn("v1");
    when(dictionary.indexOf("v1")).thenReturn(1);

    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.getDataType()).thenReturn(DataType.STRING);

    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getDictionary()).thenReturn(dictionary);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);

    EqPredicate predicate = new EqPredicate(ExpressionContext.forIdentifier(COLUMN), "v1");
    PredicateEvaluator raw =
        EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator(predicate, DataType.STRING);
    assertFalse(raw.isDictionaryBased(), "Baseline evaluator must be raw-value-based");
    assertTrue(raw instanceof BaseRawValueBasedPredicateEvaluator);

    PredicateEvaluator converted = StarTreeUtils.toDictionaryBased(raw, predicate, dataSource);
    assertTrue(converted.isDictionaryBased(), "Converted evaluator must be dictionary-based");
    assertNotNull(converted.getMatchingDictIds());
  }

  /// Dictionary-based evaluators flow through unchanged (`instanceof` no-op).
  @Test
  public void testToDictionaryBasedIsNoopForDictionaryBased() {
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.length()).thenReturn(1);
    when(dictionary.indexOf("v1")).thenReturn(0);

    DataSource dataSource = mock(DataSource.class);
    when(dataSource.getDictionary()).thenReturn(dictionary);

    EqPredicate predicate = new EqPredicate(ExpressionContext.forIdentifier(COLUMN), "v1");
    PredicateEvaluator dictBased =
        EqualsPredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, dictionary, DataType.STRING);
    assertTrue(dictBased.isDictionaryBased());

    PredicateEvaluator result = StarTreeUtils.toDictionaryBased(dictBased, predicate, dataSource);
    assertSame(result, dictBased, "Dictionary-based evaluator should pass through unchanged");
  }

  //-------------------------------------------------------------------------
  // Segment-level test: RAW forward + separated dictionary + star-tree
  //-------------------------------------------------------------------------

  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "StarTreeUtilsTest");
  private static final String TABLE_NAME = "starTreeUtilsTest";
  private static final String SEGMENT_NAME = "testSegment";
  private static final int NUM_ROWS = 1000;
  private static final int CARDINALITY = 10;

  private IndexSegment _segment;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);

    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension(COLUMN, DataType.STRING)
        .build();

    // Explicit indexes JSON:
    //   forward.encodingType = RAW  → forward index stores raw values
    //   dictionary.disabled  = false → keep the dictionary alongside
    ObjectNode indexes = JsonUtils.newObjectNode();
    ObjectNode forwardCfg = JsonUtils.newObjectNode();
    forwardCfg.put("encodingType", "RAW");
    indexes.set("forward", forwardCfg);
    ObjectNode dictCfg = JsonUtils.newObjectNode();
    dictCfg.put("disabled", false);
    indexes.set("dictionary", dictCfg);

    FieldConfig rawWithDict = new FieldConfig.Builder(COLUMN)
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withIndexes(indexes)
        .build();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setFieldConfigList(List.of(rawWithDict))
        .build();

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(COLUMN, "v" + (i % CARDINALITY));
      rows.add(row);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    SegmentGeneratorConfig cfg = new SegmentGeneratorConfig(tableConfig, schema);
    cfg.setOutDir(TEMP_DIR.getPath());
    cfg.setSegmentName(SEGMENT_NAME);
    driver.init(cfg, new GenericRowRecordReader(rows));
    driver.build();

    StarTreeIndexConfig sti =
        new StarTreeIndexConfig(List.of(COLUMN), null, List.of("COUNT__*"), null, Integer.MAX_VALUE);
    File indexDir = new File(TEMP_DIR, SEGMENT_NAME);
    try (MultipleTreesBuilder builder = new MultipleTreesBuilder(List.of(sti), false, indexDir,
        MultipleTreesBuilder.BuildMode.OFF_HEAP)) {
      builder.build();
    }

    _segment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    if (_segment != null) {
      _segment.destroy();
    }
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  /// Sanity check that the column is exactly the RAW-forward + separated-dictionary configuration the fix targets.
  @Test
  public void testColumnHasRawForwardAndDictionary() {
    DataSource dataSource = _segment.getDataSource(COLUMN);
    assertNotNull(dataSource.getDictionary(), "Expected dictionary on RAW-encoded dim column");
    assertFalse(dataSource.getForwardIndex().isDictionaryEncoded(),
        "Expected forward index to be RAW-encoded");
    assertNotNull(_segment.getStarTrees(), "Star-tree must be built");
    assertEquals(_segment.getStarTrees().size(), 1);
  }

  /// End-to-end assertion equivalent to `EXPLAIN PLAN` showing a `STAR_TREE` node:
  /// a filter query returning `COUNT(*)` from a RAW-forward + separated-dictionary star-tree dimension is served
  /// via the star-tree operator (non-null result from `createStarTreeBasedProjectOperator`) and reads a single
  /// aggregated record per matching path — same as a `DICTIONARY`-encoded dim would.
  @Test
  public void testStarTreeAcceleratesEqualityFilterOnRawWithDictionary() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        String.format("SELECT COUNT(*) FROM %s WHERE %s = 'v3'", TABLE_NAME, COLUMN));

    FilterPlanNode filterPlanNode = new FilterPlanNode(new SegmentContext(_segment), queryContext);
    filterPlanNode.run();

    BaseProjectOperator<?> operator = StarTreeUtils.createStarTreeBasedProjectOperator(_segment, queryContext,
        queryContext.getAggregationFunctions(), queryContext.getFilter(),
        filterPlanNode.getPredicateEvaluators());
    assertNotNull(operator, "Star-tree plan expected for EQ on RAW+dict dimension");

    // Traversal must not throw; before the fix, StarTreeFilterOperator.getMatchingDictIds threw UOE here.
    ValueBlock block = operator.nextBlock();
    assertNotNull(block, "Star-tree traversal returned no block");

    // Star-tree yields one aggregated document per matching path — same behavior as DICTIONARY-encoded columns.
    // (For an EQ on a single dim, matchLeafRecords=MAX_VALUE, and 1 unique matching value, exactly one leaf matches.)
    assertEquals(block.getNumDocs(), 1,
        "Expected exactly one aggregated star-tree document for the EQ filter; got " + block.getNumDocs());
  }
}
