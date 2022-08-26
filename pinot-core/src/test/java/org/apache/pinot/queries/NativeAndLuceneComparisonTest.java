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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.testng.annotations.Test;


public class NativeAndLuceneComparisonTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "NativeAndLuceneComparisonTest");
  private static final String TABLE_NAME = "MyTable";
  private static final String SEGMENT_NAME_LUCENE = "testSegmentLucene";
  private static final String SEGMENT_NAME_NATIVE = "testSegmentNative";
  private static final String DOMAIN_NAMES_COL_LUCENE = "DOMAIN_NAMES_LUCENE";
  private static final String DOMAIN_NAMES_COL_NATIVE = "DOMAIN_NAMES_NATIVE";
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

  private List<String> getDomainNames() {
    return Arrays.asList("Prince Andrew kept looking with an amused smile from Pierre",
        "vicomte and from the vicomte to their hostess. In the first moment of",
        "Pierre’s outburst Anna Pávlovna, despite her social experience, was",
        "horror-struck. But when she saw that Pierre’s sacrilegious words",
        "had not exasperated the vicomte, and had convinced herself that it was",
        "impossible to stop him, she rallied her forces and joined the vicomte in", "a vigorous attack on the orator",
        "horror-struck. But when she", "she rallied her forces and joined", "outburst Anna Pávlovna",
        "she rallied her forces and", "despite her social experience", "had not exasperated the vicomte",
        " despite her social experience", "impossible to stop him", "despite her social experience");
  }

  private List<GenericRow> createTestData(int numRows) {
    List<GenericRow> rows = new ArrayList<>();
    List<String> domainNames = getDomainNames();
    for (int i = 0; i < numRows; i++) {
      String domain = domainNames.get(i % domainNames.size());
      GenericRow row = new GenericRow();
      row.putField(DOMAIN_NAMES_COL_LUCENE, domain);
      row.putField(DOMAIN_NAMES_COL_NATIVE, domain);
      rows.add(row);
    }

    return rows;
  }

  private void buildLuceneSegment()
      throws Exception {
    List<GenericRow> rows = createTestData(NUM_ROWS);
    List<FieldConfig> fieldConfigs = new ArrayList<>();

    fieldConfigs.add(
        new FieldConfig(DOMAIN_NAMES_COL_LUCENE, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.TEXT, null,
            null));

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Arrays.asList(DOMAIN_NAMES_COL_LUCENE)).setFieldConfigList(fieldConfigs).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(DOMAIN_NAMES_COL_LUCENE, FieldSpec.DataType.STRING).build();
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
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    Map<String, String> propertiesMap = new HashMap<>();
    FSTType fstType = FSTType.NATIVE;

    propertiesMap.put(FieldConfig.TEXT_FST_TYPE, FieldConfig.TEXT_NATIVE_FST_LITERAL);

    fieldConfigs.add(
        new FieldConfig(DOMAIN_NAMES_COL_NATIVE, FieldConfig.EncodingType.DICTIONARY, FieldConfig.IndexType.TEXT, null,
            propertiesMap));

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setInvertedIndexColumns(Arrays.asList(DOMAIN_NAMES_COL_NATIVE)).setFieldConfigList(fieldConfigs).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(DOMAIN_NAMES_COL_NATIVE, FieldSpec.DataType.STRING).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME_NATIVE);
    config.setFSTIndexType(fstType);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private ImmutableSegment loadLuceneSegment()
      throws Exception {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    Set<String> textIndexCols = new HashSet<>();
    textIndexCols.add(DOMAIN_NAMES_COL_LUCENE);
    indexLoadingConfig.setTextIndexColumns(textIndexCols);
    Set<String> invertedIndexCols = new HashSet<>();
    invertedIndexCols.add(DOMAIN_NAMES_COL_LUCENE);
    indexLoadingConfig.setInvertedIndexColumns(invertedIndexCols);
    return ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME_LUCENE), indexLoadingConfig);
  }

  private ImmutableSegment loadNativeIndexSegment()
      throws Exception {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    Map<String, String> propertiesMap = new HashMap<>();
    FSTType fstType = FSTType.NATIVE;
    propertiesMap.put(FieldConfig.TEXT_FST_TYPE, FieldConfig.TEXT_NATIVE_FST_LITERAL);

    Map<String, Map<String, String>> columnPropertiesParentMap = new HashMap<>();
    Set<String> textIndexCols = new HashSet<>();
    textIndexCols.add(DOMAIN_NAMES_COL_NATIVE);
    indexLoadingConfig.setTextIndexColumns(textIndexCols);
    indexLoadingConfig.setFSTIndexType(fstType);
    Set<String> invertedIndexCols = new HashSet<>();
    invertedIndexCols.add(DOMAIN_NAMES_COL_NATIVE);
    indexLoadingConfig.setInvertedIndexColumns(invertedIndexCols);
    columnPropertiesParentMap.put(DOMAIN_NAMES_COL_NATIVE, propertiesMap);
    indexLoadingConfig.setColumnProperties(columnPropertiesParentMap);
    return ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME_NATIVE), indexLoadingConfig);
  }

  private void testSelectionResults(String nativeQuery, String luceneQuery) {
    _indexSegment = _nativeIndexSegment;
    _indexSegments = Arrays.asList(_nativeIndexSegment);
    Operator<SelectionResultsBlock> operator = getOperator(nativeQuery);
    SelectionResultsBlock operatorResult = operator.nextBlock();
    List<Object[]> resultset = (List<Object[]>) operatorResult.getRows();
    Assert.assertNotNull(resultset);

    _indexSegment = _luceneSegment;
    _indexSegments = Arrays.asList(_luceneSegment);
    operator = getOperator(luceneQuery);
    operatorResult = operator.nextBlock();
    List<Object[]> resultset2 = (List<Object[]>) operatorResult.getRows();
    Assert.assertNotNull(resultset2);

    Assert.assertEquals(resultset.size(), resultset2.size());
    for (int i = 0; i < resultset.size(); i++) {
      Object[] actualRow = resultset.get(i);
      Object[] expectedRow = resultset2.get(i);
      Assert.assertEquals(actualRow.length, expectedRow.length);
      for (int j = 0; j < actualRow.length; j++) {
        Object actualColValue = actualRow[j];
        Object expectedColValue = expectedRow[j];
        Assert.assertEquals(actualColValue, expectedColValue);
      }
    }
  }

  @Test
  public void testQueries() {
    String nativeQuery = "SELECT * FROM MyTable WHERE TEXT_CONTAINS(DOMAIN_NAMES_NATIVE, 'vico.*') LIMIT 50000";
    String luceneQuery = "SELECT * FROM MyTable WHERE TEXT_MATCH(DOMAIN_NAMES_LUCENE, 'vico*') LIMIT 50000";
    testSelectionResults(nativeQuery, luceneQuery);

    nativeQuery = "SELECT * FROM MyTable WHERE TEXT_CONTAINS(DOMAIN_NAMES_NATIVE, 'convi.*ced') LIMIT 50000";
    luceneQuery = "SELECT * FROM MyTable WHERE TEXT_MATCH(DOMAIN_NAMES_LUCENE, 'convi*ced') LIMIT 50000";
    testSelectionResults(nativeQuery, luceneQuery);

    nativeQuery = "SELECT * FROM MyTable WHERE TEXT_CONTAINS(DOMAIN_NAMES_NATIVE, 'vicomte') AND "
        + "TEXT_CONTAINS(DOMAIN_NAMES_NATIVE, 'hos.*') LIMIT 50000";
    luceneQuery = "SELECT * FROM MyTable WHERE TEXT_MATCH(DOMAIN_NAMES_LUCENE, 'vicomte AND hos*') LIMIT 50000";
    testSelectionResults(nativeQuery, luceneQuery);

    nativeQuery = "SELECT * FROM MyTable WHERE TEXT_CONTAINS(DOMAIN_NAMES_NATIVE, 'sac.*') OR "
        + "TEXT_CONTAINS(DOMAIN_NAMES_NATIVE, 'herself') LIMIT 50000";
    luceneQuery = "SELECT * FROM MyTable WHERE TEXT_MATCH(DOMAIN_NAMES_LUCENE, 'sac* OR herself') LIMIT 50000";
    testSelectionResults(nativeQuery, luceneQuery);

    nativeQuery = "SELECT * FROM MyTable WHERE TEXT_CONTAINS(DOMAIN_NAMES_NATIVE, 'vicomte') LIMIT 50000";
    luceneQuery = "SELECT * FROM MyTable WHERE TEXT_MATCH(DOMAIN_NAMES_LUCENE, 'vicomte') LIMIT 50000";
    testSelectionResults(nativeQuery, luceneQuery);
  }
}
