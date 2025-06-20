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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class NativeAndLuceneComparisonTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "NativeAndLuceneComparisonTest");
  private static final String TABLE_NAME = "MyTable";
  private static final String SEGMENT_NAME_LUCENE = "testSegmentLucene";
  private static final String SEGMENT_NAME_NATIVE = "testSegmentNative";

  private static final String QUOTES_COL_LUCENE = "QUOTES_LUCENE";
  private static final String QUOTES_COL_LUCENE_MC = "QUOTES_LUCENE_MC"; //multi-col index
  private static final String QUOTES_COL_NATIVE = "QUOTES_NATIVE";

  private static final String QUOTES_COL_LUCENE_MV = "QUOTES_LUCENE_MV";
  private static final String QUOTES_COL_LUCENE_MC_MV = "QUOTES_LUCENE_MC_MV"; //multi-col index
  private static final String QUOTES_COL_NATIVE_MV = "QUOTES_NATIVE_MV";

  private static final Integer NUM_ROWS = 1024;

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  private IndexSegment _luceneSegment;
  private IndexSegment _nativeIndexSegment;

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

    List<IndexSegment> segments = new ArrayList<>();
    buildLuceneSegment();
    buildNativeTextIndexSegment();

    _luceneSegment = loadLuceneSegment();
    _nativeIndexSegment = loadNativeIndexSegment();

    segments.add(_luceneSegment);
    segments.add(_nativeIndexSegment);

    _indexSegment = segments.get(ThreadLocalRandom.current().nextInt(2));
    _indexSegments = segments;
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private String[] getTextData() {
    return new String[]{"Prince Andrew kept looking with an amused smile from Pierre",
        "vicomte and from the vicomte to their hostess. In the first moment of",
        "Pierre’s outburst Anna Pávlovna, despite her social experience, was",
        "horror-struck. But when she saw that Pierre’s sacrilegious words",
        "had not exasperated the vicomte, and had convinced herself that it was",
        "impossible to stop him, she rallied her forces and joined the vicomte in", "a vigorous attack on the orator",
        "horror-struck. But when she", "she rallied her forces and joined", "outburst Anna Pávlovna",
        "she rallied her forces and", "despite her social experience", "had not exasperated the vicomte",
        " despite her social experience", "impossible to stop him", "despite her social experience"};
  }

  private String[][] getMVTextData() {
    return new String[][]{
        {"Prince Andrew kept", "looking with an"}, {"amused smile", "from Pierre"}, {"vicomte and from the"}, {
          "vicomte to", "their hostess."}, {"In the first moment of"}, {"Pierre’s outburst Anna Pávlovna,"}, {
          "despite her", "social", "experience, was"}, {"horror-struck.", "But when she"}, {"saw that Pierre’s"}, {
          "sacrilegious words"}, {"had not exasperated the vicomte, and had convinced herself that it was"}, {
          "impossible to stop him,", "she rallied her"}, {"forces and joined the vicomte in", "a vigorous attack on "
        + "the orator"}, {"horror-struck. But when she", "she rallied her forces and joined", "outburst Anna "
        + "Pávlovna"}, {"she rallied her forces and", "despite her social experience", "had not exasperated the "
        + "vicomte"}, {"despite her social experience", "impossible to stop him", "despite her social experience"}
    };
  }

  private List<GenericRow> createTestData(int numRows) {
    List<GenericRow> rows = new ArrayList<>();
    String[] textData = getTextData();
    String[][] mvTextData = getMVTextData();
    for (int i = 0; i < numRows; i++) {
      String doc = textData[i % textData.length];
      String[] mvDoc = mvTextData[i % mvTextData.length];
      GenericRow row = new GenericRow();
      row.putValue(QUOTES_COL_LUCENE, doc);
      row.putValue(QUOTES_COL_NATIVE, doc);
      row.putValue(QUOTES_COL_LUCENE_MC, doc);
      row.putValue(QUOTES_COL_LUCENE_MV, mvDoc);
      row.putValue(QUOTES_COL_NATIVE_MV, mvDoc);
      row.putValue(QUOTES_COL_LUCENE_MC_MV, mvDoc);
      rows.add(row);
    }

    return rows;
  }

  private void buildLuceneSegment()
      throws Exception {
    List<GenericRow> rows = createTestData(NUM_ROWS);
    List<FieldConfig> fieldConfigs = List.of(
        new FieldConfig(QUOTES_COL_LUCENE, EncodingType.DICTIONARY, FieldConfig.IndexType.TEXT, null, null),
        new FieldConfig(QUOTES_COL_LUCENE_MV, EncodingType.DICTIONARY, FieldConfig.IndexType.TEXT, null, null),
        new FieldConfig.Builder(QUOTES_COL_LUCENE_MC).withEncodingType(EncodingType.DICTIONARY).build(),
        new FieldConfig.Builder(QUOTES_COL_LUCENE_MC_MV).withEncodingType(EncodingType.DICTIONARY).build()
    );

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setInvertedIndexColumns(List.of(QUOTES_COL_LUCENE, QUOTES_COL_LUCENE_MV))
        .setFieldConfigList(fieldConfigs)
        .setMultiColumnTextIndexConfig(
            new MultiColumnTextIndexConfig(List.of(QUOTES_COL_LUCENE_MC, QUOTES_COL_LUCENE_MC_MV)))
        .build();

    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(QUOTES_COL_LUCENE, FieldSpec.DataType.STRING)
        .addMultiValueDimension(QUOTES_COL_LUCENE_MV, FieldSpec.DataType.STRING)
        .addSingleValueDimension(QUOTES_COL_LUCENE_MC, FieldSpec.DataType.STRING)
        .addMultiValueDimension(QUOTES_COL_LUCENE_MC_MV, FieldSpec.DataType.STRING)
        .build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME_LUCENE);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private void buildNativeTextIndexSegment()
      throws Exception {
    List<GenericRow> rows = createTestData(NUM_ROWS);
    List<FieldConfig> fieldConfigs = List.of(
        new FieldConfig(QUOTES_COL_NATIVE, EncodingType.DICTIONARY, FieldConfig.IndexType.TEXT, null,
            Map.of(FieldConfig.TEXT_FST_TYPE, FieldConfig.TEXT_NATIVE_FST_LITERAL)),
        new FieldConfig(QUOTES_COL_NATIVE_MV, EncodingType.DICTIONARY, FieldConfig.IndexType.TEXT, null,
            Map.of(FieldConfig.TEXT_FST_TYPE, FieldConfig.TEXT_NATIVE_FST_LITERAL)));

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(List.of(QUOTES_COL_NATIVE, QUOTES_COL_NATIVE_MV))
        .setFieldConfigList(fieldConfigs).build();
    tableConfig.getIndexingConfig().setFSTIndexType(FSTType.NATIVE);

    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName(TABLE_NAME)
        .addSingleValueDimension(QUOTES_COL_NATIVE, FieldSpec.DataType.STRING)
        .addMultiValueDimension(QUOTES_COL_NATIVE_MV, FieldSpec.DataType.STRING)
        .build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME_NATIVE);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private ImmutableSegment loadLuceneSegment()
      throws Exception {
    List<FieldConfig> fieldConfigs = List.of(
        new FieldConfig(QUOTES_COL_LUCENE, EncodingType.DICTIONARY, FieldConfig.IndexType.TEXT, null, null),
        new FieldConfig(QUOTES_COL_LUCENE_MV, EncodingType.DICTIONARY, FieldConfig.IndexType.TEXT, null, null),
        new FieldConfig.Builder(QUOTES_COL_LUCENE_MC).withEncodingType(EncodingType.DICTIONARY).build(),
        new FieldConfig.Builder(QUOTES_COL_LUCENE_MC_MV).withEncodingType(EncodingType.DICTIONARY).build()
    );

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setInvertedIndexColumns(List.of(QUOTES_COL_LUCENE, QUOTES_COL_LUCENE_MV))
        .setFieldConfigList(fieldConfigs)
        .setMultiColumnTextIndexConfig(
            new MultiColumnTextIndexConfig(List.of(QUOTES_COL_LUCENE_MC, QUOTES_COL_LUCENE_MC_MV)))
        .build();

    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(QUOTES_COL_LUCENE, FieldSpec.DataType.STRING)
        .addMultiValueDimension(QUOTES_COL_LUCENE_MV, FieldSpec.DataType.STRING)
        .addSingleValueDimension(QUOTES_COL_LUCENE_MC, FieldSpec.DataType.STRING)
        .addMultiValueDimension(QUOTES_COL_LUCENE_MC_MV, FieldSpec.DataType.STRING)
        .build();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, schema);
    return ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME_LUCENE), indexLoadingConfig);
  }

  private ImmutableSegment loadNativeIndexSegment()
      throws Exception {
    List<FieldConfig> fieldConfigs = List.of(
        new FieldConfig(QUOTES_COL_NATIVE, EncodingType.DICTIONARY, FieldConfig.IndexType.TEXT, null,
            Map.of(FieldConfig.TEXT_FST_TYPE, FieldConfig.TEXT_NATIVE_FST_LITERAL)),
        new FieldConfig(QUOTES_COL_NATIVE_MV, EncodingType.DICTIONARY, FieldConfig.IndexType.TEXT, null,
            Map.of(FieldConfig.TEXT_FST_TYPE, FieldConfig.TEXT_NATIVE_FST_LITERAL)));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(List.of(QUOTES_COL_NATIVE, QUOTES_COL_NATIVE_MV)).setFieldConfigList(fieldConfigs)
        .build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(QUOTES_COL_NATIVE, FieldSpec.DataType.STRING)
        .addMultiValueDimension(QUOTES_COL_NATIVE_MV, FieldSpec.DataType.STRING).build();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, schema);
    return ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME_NATIVE), indexLoadingConfig);
  }

  private void testSelectionResults(String nativeQuery, String luceneQuery) {
    _indexSegment = _nativeIndexSegment;
    _indexSegments = List.of(_nativeIndexSegment);
    Operator<SelectionResultsBlock> operator = getOperator(nativeQuery);
    SelectionResultsBlock operatorResult = operator.nextBlock();
    List<Object[]> nativeResult = (List<Object[]>) operatorResult.getRows();
    Assert.assertNotNull(nativeResult);

    _indexSegment = _luceneSegment;
    _indexSegments = List.of(_luceneSegment);
    operator = getOperator(luceneQuery);
    operatorResult = operator.nextBlock();
    List<Object[]> luceneResult = (List<Object[]>) operatorResult.getRows();
    Assert.assertNotNull(luceneResult);

    Assert.assertEquals(nativeResult.size(), luceneResult.size());
    for (int i = 0; i < nativeResult.size(); i++) {
      Object[] actualRow = nativeResult.get(i);
      Object[] expectedRow = luceneResult.get(i);
      Assert.assertEquals(actualRow.length, expectedRow.length);
      for (int j = 0; j < actualRow.length; j++) {
        Object actualColValue = actualRow[j];
        Object expectedColValue = expectedRow[j];
        Assert.assertEquals(actualColValue, expectedColValue);
      }
    }
  }

  @DataProvider(name = "isMultiColIndex")
  public static Boolean[] isMultiColIndex() {
    return new Boolean[]{false, true};
  }

  @Test(dataProvider = "isMultiColIndex")
  public void testQueries(boolean isMultiColIndex) {
    String columns = isMultiColIndex ? QUOTES_COL_LUCENE_MC + "," + QUOTES_COL_LUCENE_MC_MV
        : QUOTES_COL_LUCENE + "," + QUOTES_COL_LUCENE_MV;
    String matchColumn = isMultiColIndex ? QUOTES_COL_LUCENE_MC : QUOTES_COL_LUCENE;

    String nativeQuery = "SELECT * FROM MyTable WHERE TEXT_CONTAINS(QUOTES_NATIVE, 'vico.*') LIMIT 50000";
    String luceneQuery =
        "SELECT " + columns + " FROM MyTable WHERE TEXT_MATCH(" + matchColumn + ", 'vico*') LIMIT 50000";
    testSelectionResults(nativeQuery, luceneQuery);

    nativeQuery = "SELECT * FROM MyTable WHERE TEXT_CONTAINS(QUOTES_NATIVE, 'convi.*ced') LIMIT 50000";
    luceneQuery = "SELECT " + columns + " FROM MyTable WHERE TEXT_MATCH(" + matchColumn + ", 'convi*ced') LIMIT 50000";
    testSelectionResults(nativeQuery, luceneQuery);

    nativeQuery = "SELECT * FROM MyTable WHERE TEXT_CONTAINS(QUOTES_NATIVE, 'vicomte') AND "
        + "TEXT_CONTAINS(QUOTES_NATIVE, 'hos.*') LIMIT 50000";
    luceneQuery =
        "SELECT " + columns + " FROM MyTable WHERE TEXT_MATCH(" + matchColumn + ", 'vicomte AND hos*') LIMIT 50000";
    testSelectionResults(nativeQuery, luceneQuery);

    nativeQuery = "SELECT * FROM MyTable WHERE TEXT_CONTAINS(QUOTES_NATIVE, 'sac.*') OR "
        + "TEXT_CONTAINS(QUOTES_NATIVE, 'herself') LIMIT 50000";
    luceneQuery =
        "SELECT " + columns + " FROM MyTable WHERE TEXT_MATCH(" + matchColumn + ", 'sac* OR herself') LIMIT 50000";
    testSelectionResults(nativeQuery, luceneQuery);

    nativeQuery = "SELECT * FROM MyTable WHERE TEXT_CONTAINS(QUOTES_NATIVE, 'vicomte') LIMIT 50000";
    luceneQuery = "SELECT " + columns + " FROM MyTable WHERE TEXT_MATCH(" + matchColumn + ", 'vicomte') LIMIT 50000";
    testSelectionResults(nativeQuery, luceneQuery);

    matchColumn = isMultiColIndex ? QUOTES_COL_LUCENE_MC_MV : QUOTES_COL_LUCENE_MV;

    String nativeMVQuery = "SELECT * FROM MyTable WHERE TEXT_CONTAINS(QUOTES_NATIVE_MV, 'vico.*') LIMIT 50000";
    String luceneMVQuery =
        "SELECT " + columns + " FROM MyTable WHERE TEXT_MATCH(" + matchColumn + ", 'vico*') LIMIT 50000";
    testSelectionResults(nativeMVQuery, luceneMVQuery);

    nativeMVQuery = "SELECT * FROM MyTable WHERE TEXT_CONTAINS(QUOTES_NATIVE_MV, 'convi.*ced') LIMIT 50000";
    luceneMVQuery =
        "SELECT " + columns + " FROM MyTable WHERE TEXT_MATCH(" + matchColumn + ", 'convi*ced') LIMIT 50000";
    testSelectionResults(nativeMVQuery, luceneMVQuery);

    nativeMVQuery = "SELECT * FROM MyTable WHERE TEXT_CONTAINS(QUOTES_NATIVE_MV, 'vicomte') AND "
        + "TEXT_CONTAINS(QUOTES_NATIVE_MV, 'hos.*') LIMIT 50000";
    luceneMVQuery =
        "SELECT " + columns + " FROM MyTable WHERE TEXT_MATCH(" + matchColumn + ", 'vicomte AND hos*') LIMIT 50000";
    testSelectionResults(nativeMVQuery, luceneMVQuery);

    nativeMVQuery = "SELECT * FROM MyTable WHERE TEXT_CONTAINS(QUOTES_NATIVE_MV, 'sac.*') OR "
        + "TEXT_CONTAINS(QUOTES_NATIVE_MV, 'herself') LIMIT 50000";
    luceneMVQuery =
        "SELECT " + columns + " FROM MyTable WHERE TEXT_MATCH(" + matchColumn + ", 'sac* OR herself') LIMIT 50000";
    testSelectionResults(nativeMVQuery, luceneMVQuery);

    nativeMVQuery = "SELECT * FROM MyTable WHERE TEXT_CONTAINS(QUOTES_NATIVE_MV, 'vicomte') LIMIT 50000";
    luceneMVQuery = "SELECT " + columns + " FROM MyTable WHERE TEXT_MATCH(" + matchColumn + ", 'vicomte') LIMIT 50000";
    testSelectionResults(nativeMVQuery, luceneMVQuery);
  }
}
