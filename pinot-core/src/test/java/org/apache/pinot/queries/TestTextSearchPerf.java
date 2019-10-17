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

import com.google.common.base.Stopwatch;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.data.readers.RecordReader;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.SelectionOnlyOperator;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestTextSearchPerf extends BaseQueriesTest {

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "TextSearchPerf");
  private static final String TABLE_NAME = "MyTable";
  private static final String SEGMENT_NAME = TABLE_NAME + "_100000000_200000000";
  private static final String INT_COL_NAME = "INT_COL";
  private static final String ACCESS_LOG_TEXT_COL_NAME = "ACCESS_LOG_TEXT_COL";
  private static final String ACCESS_LOG_STRING_COL_NAME = "ACCESS_LOG_STRING_COL";
  private static final String QUERY_LOG_TEXT_COL_NAME = "QUERY_LOG_TEXT_COL";
  private static final String QUERY_LOG_STRING_COL_NAME = "QUERY_LOG_STRING_COL";
  private static final String SKILLS_TEXT_COL_NAME = "SKILLS_TEXT_COL";
  private static final String SKILLS_STRING_COL_NAME = "SKILLS_STRING_COL";
  private static final List<String> textSearchColumns = new ArrayList<>();
  private static final List<String> rawIndexCreationColumns = new ArrayList<>();
  private static final int INT_BASE_VALUE = 1000;
  private List<GenericRow> _rows = new ArrayList<>();
  private RecordReader _recordReader;
  Schema _schema;

  private List<IndexSegment> _indexSegments = new ArrayList<>(1);
  private List<SegmentDataManager> _segmentDataManagers = new ArrayList<>();

  @BeforeClass
  public void setUp()
      throws Exception {
    createPinotTableSchema();
    createTestData();
    _recordReader = new GenericRowRecordReader(_rows, _schema);
    createSegment();
    loadSegment();
  }

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegments.get(0);
  }

  @Override
  protected List<SegmentDataManager> getSegmentDataManagers() {
    return _segmentDataManagers;
  }

  @AfterClass
  public void tearDown() {
    for (IndexSegment indexSegment : _indexSegments) {
      if (indexSegment != null) {
        indexSegment.destroy();
      }
    }
    _indexSegments.clear();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private void createPinotTableSchema() {
    _schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(ACCESS_LOG_STRING_COL_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(ACCESS_LOG_TEXT_COL_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(QUERY_LOG_TEXT_COL_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(QUERY_LOG_STRING_COL_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(SKILLS_TEXT_COL_NAME, FieldSpec.DataType.STRING)
        .addSingleValueDimension(SKILLS_STRING_COL_NAME, FieldSpec.DataType.STRING)
        .addMetric(INT_COL_NAME, FieldSpec.DataType.INT).build();
  }

  private void createSegment()
      throws Exception {
    textSearchColumns.add(ACCESS_LOG_TEXT_COL_NAME);
    textSearchColumns.add(QUERY_LOG_TEXT_COL_NAME);
    textSearchColumns.add(SKILLS_TEXT_COL_NAME);
    rawIndexCreationColumns.addAll(textSearchColumns);
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(_schema);
    segmentGeneratorConfig.setTableName(TABLE_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setRawIndexCreationColumns(rawIndexCreationColumns);
    segmentGeneratorConfig.setTextSearchColumns(textSearchColumns);
    segmentGeneratorConfig.setCheckTimeColumnValidityDuringGeneration(false);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, _recordReader);
    driver.build();

    File segmentIndexDir = new File(INDEX_DIR.getAbsolutePath(), SEGMENT_NAME);
    if (!segmentIndexDir.exists()) {
      throw new IllegalStateException("Segment generation failed");
    }
  }

  private void loadSegment()
      throws Exception {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    Set<String> textColumns = new HashSet<>();
    textColumns.addAll(textSearchColumns);
    indexLoadingConfig.setTextSearchColumns(textColumns);
    ImmutableSegment segment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
    _indexSegments.add(segment);
  }

  private void createTestData()
      throws Exception {
    // read the skills file
    URL resourceUrl = getClass().getClassLoader().getResource("data/text_search_data/skills.txt");
    File skillFile = new File(resourceUrl.getFile());
    String[] skills = new String[100];
    int skillCount = 0;
    try (InputStream inputStream = new FileInputStream(skillFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while ((line = reader.readLine()) != null) {
        skills[skillCount++] = line;
      }
    }

    // read the pql query log file
    resourceUrl = getClass().getClassLoader().getResource("data/text_search_data/pql_query1.txt");
    File logFile = new File(resourceUrl.getFile());
    String[] queries = new String[30000];
    int queryCount = 0;
    try (InputStream inputStream = new FileInputStream(logFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while ((line = reader.readLine()) != null) {
        queries[queryCount++] = line;
      }
    }

    // apache access log has 1.3million log lines
    resourceUrl = getClass().getClassLoader().getResource("data/text_search_data/apache_access.txt");
    logFile = new File(resourceUrl.getFile());
    Random random = new Random();
    try (InputStream inputStream = new FileInputStream(logFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      int counter = 0;
      while ((line = reader.readLine()) != null) {
        GenericRow row = new GenericRow();
        row.putField(INT_COL_NAME, INT_BASE_VALUE + counter);
        row.putField(ACCESS_LOG_STRING_COL_NAME, line);
        row.putField(ACCESS_LOG_TEXT_COL_NAME, line);
        if (counter >= skillCount) {
          int index = random.nextInt(skillCount);
          row.putField(SKILLS_TEXT_COL_NAME, skills[index]);
          row.putField(SKILLS_STRING_COL_NAME, skills[index]);
        } else {
          row.putField(SKILLS_TEXT_COL_NAME, skills[counter]);
          row.putField(SKILLS_STRING_COL_NAME, skills[counter]);
        }
        if (counter >= queryCount) {
          int index = random.nextInt(queryCount);
          row.putField(QUERY_LOG_TEXT_COL_NAME, queries[index]);
          row.putField(QUERY_LOG_STRING_COL_NAME, queries[index]);
        } else {
          row.putField(QUERY_LOG_TEXT_COL_NAME, queries[counter]);
          row.putField(QUERY_LOG_STRING_COL_NAME, queries[counter]);
        }
        _rows.add(row);
        counter++;
      }
    }
  }

  // TODO: move all these to JMH

  @Test
  public void testPerf1() {
    String luceneQuery = "SELECT INT_COL FROM MyTable WHERE text_match(QUERY_LOG_TEXT_COL, '\"GROUP BY\"') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL FROM MyTable WHERE regexp_like(QUERY_LOG_STRING_COL, 'GROUP BY') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf2() {
    String luceneQuery = "SELECT INT_COL FROM MyTable WHERE text_match(QUERY_LOG_TEXT_COL, '\"timestamp between\" AND \"GROUP BY\"') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL FROM MyTable WHERE regexp_like(QUERY_LOG_STRING_COL, 'timestamp between.*GROUP BY') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf3() {
    String luceneQuery = "SELECT INT_COL FROM MyTable WHERE text_match(QUERY_LOG_TEXT_COL, 'in') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL FROM MyTable WHERE regexp_like(QUERY_LOG_STRING_COL, 'in') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf4() {
    String luceneQuery = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE text_match(SKILLS_TEXT_COL, '/.*Exception/') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL, SKILLS_STRING_COL FROM MyTable WHERE regexp_like(SKILLS_STRING_COL, 'Exception') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf5() {
    String luceneQuery = "SELECT INT_COL, ACCESS_LOG_TEXT_COL FROM MyTable WHERE text_match(ACCESS_LOG_TEXT_COL, '/.*slideshow/') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL, ACCESS_LOG_STRING_COL FROM MyTable WHERE regexp_like(ACCESS_LOG_STRING_COL, 'slideshow') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf6() {
    String luceneQuery = "SELECT INT_COL, ACCESS_LOG_TEXT_COL FROM MyTable WHERE text_match(ACCESS_LOG_TEXT_COL, '/.*webkit/') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL, ACCESS_LOG_STRING_COL FROM MyTable WHERE regexp_like(ACCESS_LOG_STRING_COL, 'webkit') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf7() {
    String luceneQuery = "SELECT INT_COL, ACCESS_LOG_TEXT_COL FROM MyTable WHERE text_match(ACCESS_LOG_TEXT_COL, '/.*webkit/ AND Chrome') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL, ACCESS_LOG_STRING_COL FROM MyTable WHERE regexp_like(ACCESS_LOG_STRING_COL, 'webkit.*Chrome|Chrome.*webkit') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf8() {
    String luceneQuery = "SELECT INT_COL, ACCESS_LOG_TEXT_COL FROM MyTable WHERE text_match(ACCESS_LOG_TEXT_COL, '109.169.248.247') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL, ACCESS_LOG_STRING_COL FROM MyTable WHERE regexp_like(ACCESS_LOG_STRING_COL, '109\\.169\\.248\\.247') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf9() {
    String luceneQuery = "SELECT INT_COL, ACCESS_LOG_TEXT_COL FROM MyTable WHERE text_match(ACCESS_LOG_TEXT_COL, '/.*gallery/') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL, ACCESS_LOG_STRING_COL FROM MyTable WHERE regexp_like(ACCESS_LOG_STRING_COL, 'gallery') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf10() {
    String luceneQuery = "SELECT INT_COL, ACCESS_LOG_TEXT_COL FROM MyTable WHERE text_match(ACCESS_LOG_TEXT_COL, 'Firefox') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL, ACCESS_LOG_STRING_COL FROM MyTable WHERE regexp_like(ACCESS_LOG_STRING_COL, 'Firefox') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf11() {
    String luceneQuery = "SELECT INT_COL, ACCESS_LOG_TEXT_COL FROM MyTable WHERE text_match(ACCESS_LOG_TEXT_COL, '/.*administrator/ AND Firefox') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL, ACCESS_LOG_STRING_COL FROM MyTable WHERE regexp_like(ACCESS_LOG_STRING_COL, 'administrator.*Firefox|Firefox.*administrator') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf12() {
    String luceneQuery = "SELECT INT_COL, ACCESS_LOG_TEXT_COL FROM MyTable WHERE text_match(ACCESS_LOG_TEXT_COL, 'administrator AND Firefox') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL, ACCESS_LOG_STRING_COL FROM MyTable WHERE regexp_like(ACCESS_LOG_STRING_COL, 'administrator.*Firefox') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf13() {
    String luceneQuery = "SELECT INT_COL, ACCESS_LOG_TEXT_COL FROM MyTable WHERE text_match(ACCESS_LOG_TEXT_COL, '\"Windows NT\"') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL, ACCESS_LOG_STRING_COL FROM MyTable WHERE regexp_like(ACCESS_LOG_STRING_COL, 'Windows NT') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf14() {
    String luceneQuery = "SELECT INT_COL, ACCESS_LOG_TEXT_COL FROM MyTable WHERE text_match(ACCESS_LOG_TEXT_COL, 'POST') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL, ACCESS_LOG_STRING_COL FROM MyTable WHERE regexp_like(ACCESS_LOG_STRING_COL, 'POST') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf15() {
    String luceneQuery = "SELECT INT_COL, ACCESS_LOG_TEXT_COL FROM MyTable WHERE text_match(ACCESS_LOG_TEXT_COL, 'POST AND /.*administrator/') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL, ACCESS_LOG_STRING_COL FROM MyTable WHERE regexp_like(ACCESS_LOG_STRING_COL, 'POST.*administrator|administrator.*POST') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf16() {
    String luceneQuery = "SELECT INT_COL, ACCESS_LOG_TEXT_COL FROM MyTable WHERE text_match(ACCESS_LOG_TEXT_COL, '\"POST administrator\"') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL, ACCESS_LOG_STRING_COL FROM MyTable WHERE regexp_like(ACCESS_LOG_STRING_COL, 'POST /administrator') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf17() {
    String luceneQuery = "SELECT INT_COL, ACCESS_LOG_TEXT_COL FROM MyTable WHERE text_match(ACCESS_LOG_TEXT_COL, 'GET AND /.*administrator/ AND firefox') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL, ACCESS_LOG_STRING_COL FROM MyTable WHERE regexp_like(ACCESS_LOG_STRING_COL, 'GET.*administrator.*Firefox|Firefox.*GET.*administrator') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf18() {
    String luceneQuery = "SELECT INT_COL, ACCESS_LOG_TEXT_COL FROM MyTable WHERE text_match(ACCESS_LOG_TEXT_COL, '\"GET administrator\" AND firefox') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL, ACCESS_LOG_STRING_COL FROM MyTable WHERE regexp_like(ACCESS_LOG_STRING_COL, 'GET /administrator.*Firefox') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf19() {
    String luceneQuery = "SELECT INT_COL, ACCESS_LOG_TEXT_COL FROM MyTable WHERE text_match(ACCESS_LOG_TEXT_COL, '\"propecia-kaufen.de.tl\"') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL, ACCESS_LOG_STRING_COL FROM MyTable WHERE regexp_like(ACCESS_LOG_STRING_COL, 'propecia-kaufen\\.de\\.tl') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf20() {
    String luceneQuery = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE text_match(SKILLS_TEXT_COL, '\"distributed systems\" AND apache') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL, SKILLS_STRING_COL FROM MyTable WHERE regexp_like(SKILLS_STRING_COL, 'distributed systems.*apache|apache.*distributed systems') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf21() {
    String luceneQuery = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE text_match(SKILLS_TEXT_COL, 'stream*') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL, SKILLS_STRING_COL FROM MyTable WHERE regexp_like(SKILLS_STRING_COL, 'stream') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }

  @Test
  public void testPerf22() {
    String luceneQuery = "SELECT INT_COL, SKILLS_TEXT_COL FROM MyTable WHERE text_match(SKILLS_TEXT_COL, 'spark AND \"machine learning\"') LIMIT 1000000";
    Stopwatch luceneWatch = Stopwatch.createUnstarted();
    benchmarkHelper(luceneQuery, luceneWatch, 5);

    String regexQuery = "SELECT INT_COL, SKILLS_STRING_COL FROM MyTable WHERE regexp_like(SKILLS_STRING_COL, 'machine learning.*spark|spark.*machine learning') LIMIT 1000000";
    Stopwatch regexWatch = Stopwatch.createUnstarted();
    benchmarkHelper(regexQuery, regexWatch, 5);
  }


  @Test
  public void testCorrectness() {
    String query = "SELECT INT_COL, ACCESS_LOG_TEXT_COL FROM MyTable WHERE text_match(ACCESS_LOG_TEXT_COL, '\"GET administrator\" AND firefox') LIMIT 1000000";
    String query1 = "SELECT INT_COL, ACCESS_LOG_STRING_COL FROM MyTable WHERE regexp_like(ACCESS_LOG_STRING_COL, 'GET /administrator.*Firefox') LIMIT 1000000";
    SelectionOnlyOperator operator1 = getOperatorForQuery(query);
    IntermediateResultsBlock block1 = operator1.nextBlock();
    SelectionOnlyOperator operator2 = getOperatorForQuery(query1);
    IntermediateResultsBlock block2 = operator2.nextBlock();
    System.out.println("dfef");
  }

  private void benchmarkHelper(String query, Stopwatch stopwatch, int runs) {
    System.out.println("Cumulative elapsed time over " + runs + " runs for query: " + query);
    for (int i = 0; i < runs; i++) {
      BaseOperator segmentRootOperator = getOperatorForQuery(query);
      benchmark(segmentRootOperator, stopwatch);
      System.out.println("Elapsed time: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms");
    }
    System.out.println("Average elapsed time: " + ((double)stopwatch.elapsed(TimeUnit.MILLISECONDS))/runs + "ms");
  }

  private void benchmark(BaseOperator operator, Stopwatch stopwatch) {
    stopwatch.start();
    operator.nextBlock();
    stopwatch.stop();
  }
}